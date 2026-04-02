from __future__ import annotations

import asyncio
from dataclasses import asdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from difflib import SequenceMatcher
import gzip
import hashlib
from html import escape
import json
import logging
import math
import os
from pathlib import Path
import re
from html import unescape
from html.parser import HTMLParser
from urllib.parse import quote, quote_plus, urlencode, urljoin
from urllib.request import Request, urlopen

from pydantic import BaseModel, Field

try:
    from openai import OpenAI
except ImportError:  # pragma: no cover - optional dependency in local dev
    OpenAI = None


CATEGORY_ORDER = (
    "New England IPA",
    "IPA",
    "Pastry Sour Ale",
    "Sour Ale",
    "Weizen",
    "Безалкогольное",
)

CATEGORY_EMOJI = {
    "New England IPA": "🇺🇸",
    "IPA": "🌲",
    "Pastry Sour Ale": "🥧",
    "Sour Ale": "🍓",
    "Weizen": "🇩🇪",
    "Безалкогольное": "🚫",
}

FLAVOR_HINT_TOKENS = {
    "клубника",
    "манго",
    "малина",
    "вишня",
    "черника",
    "смородина",
    "банан",
    "ваниль",
    "цитрус",
    "маракуйя",
    "персик",
    "ананас",
    "лайм",
    "лимон",
    "базилик",
    "огурец",
    "арбуз",
    "дыня",
    "tropical",
    "citrus",
    "berry",
    "strawberry",
    "mango",
    "raspberry",
    "peach",
    "pineapple",
    "banana",
    "vanilla",
    "passionfruit",
}

LOGGER = logging.getLogger(__name__)
CHANNEL_URL = "https://t.me/s/beerhounds73"
UNTAPPD_SEARCH_URL = "https://untappd.com/search?q={query}"
GLIDE_PUBLISHED_DATA_URL = (
    "https://firestore.googleapis.com/v1/projects/glide-prod/"
    "databases/(default)/documents/glide-apps-v4-data/{app_id}"
)
GLIDE_TABLE_ROWS_URL = (
    "https://firestore.googleapis.com/v1/projects/glide-prod/"
    "databases/(default)/documents/glide-apps-v4-data/{app_id}/tables/{table_doc_id}/rows"
)
CACHE_PATH = Path("data/beer_inventory_cache.json")
MAX_CACHE_BYTES = 500 * 1024 * 1024


@dataclass(slots=True)
class BeerEntry:
    name: str
    brewery: str | None
    style: str
    rating: float
    rating_count: int
    alc: str | None = None
    flavor_notes: str | None = None
    untappd_url: str | None = None
    rating_available: bool = True
    untappd_abv: float | None = None
    untappd_ibu: int | None = None


@dataclass(slots=True)
class GlideListing:
    name: str
    brewery: str | None = None
    style: str | None = None
    untappd_url: str | None = None
    rating_hint: float | None = None
    alc: str | None = None
    flavor_notes: str | None = None


@dataclass(slots=True)
class UntappdSearchResult:
    name: str
    brewery: str | None
    style: str
    url: str


@dataclass(slots=True)
class UntappdBeerPage:
    rating: float
    rating_count: int
    abv: float | None = None
    ibu: int | None = None


@dataclass(slots=True)
class BeerSearchQuery:
    raw_text: str
    categories: tuple[str, ...] = ()
    exclude_categories: tuple[str, ...] = ()
    max_alc: float | None = None
    min_rating: float | None = None
    tokens: tuple[str, ...] = ()


class _LLMBeerSearchQuery(BaseModel):
    categories: list[str] = Field(default_factory=list)
    exclude_categories: list[str] = Field(default_factory=list)
    max_alc: float | None = None
    min_rating: float | None = None
    flavor_tokens: list[str] = Field(default_factory=list)
    hop_tokens: list[str] = Field(default_factory=list)
    reasoning_note: str | None = None


class _LLMBeerRerankResult(BaseModel):
    selected_ids: list[int] = Field(default_factory=list)
    reasoning_note: str | None = None


def categorize_style(style: str, alc: str | None = None) -> str | None:
    normalized = style.lower()

    if any(
        marker in normalized
        for marker in (
            "безалкоголь",
            "non-alco",
            "non-alcohol",
            "non alcoholic",
            "alcohol-free",
            "alcohol free",
            "0.0",
            "0%",
        )
    ):
        return "Безалкогольное"

    alc_value = _parse_alc_value(alc)
    if alc_value is not None and alc_value < 1:
        return "Безалкогольное"

    is_sour_family = (
        "sour" in normalized
        or "wild ale" in normalized
        or "gose" in normalized
    )

    if is_sour_family and (
        "smoothie" in normalized or "pastry" in normalized or "milkshake" in normalized
    ):
        return "Pastry Sour Ale"

    if any(marker in normalized for marker in ("new england", "hazy", "neipa")):
        return "New England IPA"

    if is_sour_family:
        return "Sour Ale"

    if any(
        marker in normalized
        for marker in ("weizen", "wheat", "hefeweizen", "witbier", "white ale")
    ):
        return "Weizen"

    if "ipa" in normalized:
        return "IPA"

    return None


def weighted_score(entry: BeerEntry) -> float:
    return entry.rating * math.log10(entry.rating_count + 10)


_GLIDE_URL_RE = re.compile(r'https://go\.glideapps\.com/play/[^"\'>\s]+')
_JSON_SCRIPT_RE = re.compile(
    r"<script[^>]*type=['\"]application/json['\"][^>]*>(.*?)</script>",
    re.IGNORECASE | re.DOTALL,
)
_UNTAPPD_JSON_LD_RE = re.compile(
    r"<script[^>]*type=['\"]application/ld\+json['\"][^>]*>(.*?)</script>",
    re.IGNORECASE | re.DOTALL,
)
_UNTAPPD_META_RE = re.compile(
    r"rating of\s+([0-9]+(?:\.[0-9]+)?)\s+out of 5,\s+with\s+([\d,]+)\s+ratings",
    re.IGNORECASE,
)
_UNTAPPD_ABV_RE = re.compile(r"([0-9]+(?:\.[0-9]+)?)\s*%\s*ABV", re.IGNORECASE)
_UNTAPPD_IBU_RE = re.compile(r"([0-9]+)\s*IBU", re.IGNORECASE)
_TAG_RE = re.compile(r"<[^>]+>")
_DATA_ATTR_RE = re.compile(
    r"<[^>]*data-name=['\"]([^'\"]+)['\"][^>]*data-brewery=['\"]([^'\"]*)['\"][^>]*>",
    re.IGNORECASE,
)


class _TelegramGlideURLParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._wrap_depth = 0
        self._current_wrap_url: str | None = None
        self._result: str | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if self._result is not None:
            return

        attrs_map = {name: value or "" for name, value in attrs}

        if tag == "div":
            classes = attrs_map.get("class", "").split()
            if self._wrap_depth == 0 and "tgme_widget_message_wrap" in classes:
                self._wrap_depth = 1
                self._current_wrap_url = None
                return
            if self._wrap_depth > 0:
                self._wrap_depth += 1
                return

        if self._wrap_depth > 0 and tag == "a":
            href = attrs_map.get("href", "")
            match = _GLIDE_URL_RE.search(href)
            if match:
                self._current_wrap_url = match.group(0)

    def handle_endtag(self, tag: str) -> None:
        if self._result is not None:
            return

        if tag == "div" and self._wrap_depth > 0:
            self._wrap_depth -= 1
            if self._wrap_depth == 0 and self._current_wrap_url is not None:
                self._result = self._current_wrap_url

    @property
    def result(self) -> str | None:
        return self._result


class _UntappdSearchResultParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.results: list[UntappdSearchResult] = []
        self._item_depth = 0
        self._tag_fields: list[str | None] = []
        self._active_fields: list[str] = []
        self._current_name_parts: list[str] = []
        self._current_brewery_parts: list[str] = []
        self._current_style_parts: list[str] = []
        self._current_url: str | None = None
        self._name_link_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attrs_map = {name: value or "" for name, value in attrs}
        classes = attrs_map.get("class", "").split()

        if self._item_depth == 0:
            if "beer-item" in classes:
                self._item_depth = 1
                self._current_name_parts = []
                self._current_brewery_parts = []
                self._current_style_parts = []
                self._current_url = None
                self._name_link_depth = 0
            return

        self._item_depth += 1

        field: str | None = None
        if "name" in classes:
            field = "name"
        elif "brewery" in classes:
            field = "brewery"
        elif "style" in classes:
            field = "style"

        self._tag_fields.append(field)
        if field is not None:
            self._active_fields.append(field)

        if tag == "a" and "name" in self._active_fields and self._current_url is None:
            href = attrs_map.get("href")
            if href:
                self._current_url = urljoin("https://untappd.com", href)
        if tag == "a" and "name" in self._active_fields:
            self._name_link_depth += 1

    def handle_data(self, data: str) -> None:
        if self._item_depth == 0 or not self._active_fields:
            return

        field = self._active_fields[-1]
        if field == "name":
            if self._name_link_depth > 0:
                self._current_name_parts.append(data)
        elif field == "brewery":
            self._current_brewery_parts.append(data)
        elif field == "style":
            self._current_style_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if self._item_depth == 0:
            return

        if tag == "a" and self._name_link_depth > 0:
            self._name_link_depth -= 1

        if self._item_depth == 1:
            self._finalize_current_item()
            self._item_depth = 0
            self._tag_fields.clear()
            self._active_fields.clear()
            return

        self._item_depth -= 1
        field = self._tag_fields.pop() if self._tag_fields else None
        if field is None:
            return

        for index in range(len(self._active_fields) - 1, -1, -1):
            if self._active_fields[index] == field:
                del self._active_fields[index]
                break

    def _finalize_current_item(self) -> None:
        name = _clean_text("".join(self._current_name_parts))
        brewery = _clean_text("".join(self._current_brewery_parts)) or None
        style = _clean_text("".join(self._current_style_parts))

        if not name or not style or not self._current_url:
            return

        self.results.append(
            UntappdSearchResult(
                name=name,
                brewery=brewery,
                style=style,
                url=self._current_url,
            )
        )


def extract_latest_glide_url(html: str) -> str | None:
    parser = _TelegramGlideURLParser()
    parser.feed(html)
    return parser.result


def extract_glide_app_id(glide_url: str) -> str | None:
    match = re.search(r"/play/([A-Za-z0-9]+)", glide_url)
    if match is None:
        return None
    return match.group(1)


def parse_glide_listings(html: str) -> list[GlideListing]:
    for raw_json in _JSON_SCRIPT_RE.findall(html):
        try:
            payload = json.loads(unescape(raw_json).strip())
        except json.JSONDecodeError:
            continue

        listings = _find_glide_listings(payload)
        if listings:
            return listings

    listings: list[GlideListing] = []
    seen: set[tuple[str, str]] = set()
    for name, brewery in _DATA_ATTR_RE.findall(html):
        key = (_normalize_text(name), _normalize_text(brewery))
        if key in seen:
            continue
        seen.add(key)
        listings.append(GlideListing(name=_clean_text(name), brewery=_clean_text(brewery) or None))
    return listings


def parse_untappd_search_results(html: str) -> list[UntappdSearchResult]:
    parser = _UntappdSearchResultParser()
    parser.feed(html)
    return parser.results


def extract_inventory_table_doc_id(document_json: str) -> str | None:
    payload = json.loads(document_json)
    schema_value = (
        payload.get("fields", {})
        .get("schema", {})
        .get("stringValue")
    )
    if not isinstance(schema_value, str):
        return None

    schema = _decode_published_schema(schema_value)
    if not isinstance(schema, dict):
        return None

    tables = schema.get("tables")
    if not isinstance(tables, list):
        return None

    for table in tables:
        if not isinstance(table, dict):
            continue
        columns = table.get("columns")
        if not isinstance(columns, list):
            continue

        column_names = {
            column.get("name")
            for column in columns
            if isinstance(column, dict) and isinstance(column.get("name"), str)
        }
        if {
            "ПИВОВАРНЯ",
            "НАЗВАНИЕ",
            "СТИЛЬ",
            "ДОСТУПНО В БАРЕ",
        } <= column_names:
            return _encode_table_doc_id(table.get("name"))

    return None


def parse_firestore_inventory_rows(rows_json: str) -> list[GlideListing]:
    payload = json.loads(rows_json)
    documents = payload.get("documents")
    if not isinstance(documents, list):
        return []

    listings: list[GlideListing] = []
    seen: set[tuple[str, str]] = set()

    for document in documents:
        if not isinstance(document, dict):
            continue
        fields = _decode_firestore_fields(document.get("fields", {}))
        if not isinstance(fields, dict):
            continue
        if fields.get("ДОСТУПНО В БАРЕ") is not True:
            continue

        name = _clean_text(str(fields.get("НАЗВАНИЕ", "")))
        if not name:
            continue

        brewery = _clean_text(str(fields.get("ПИВОВАРНЯ", ""))) or None
        key = (_normalize_text(name), _normalize_text(brewery or ""))
        if key in seen:
            continue
        seen.add(key)

        style = _clean_text(str(fields.get("СТИЛЬ", ""))) or None
        untappd_url = _clean_text(str(fields.get("ОТКРЫТЬ В UNTAPPD", ""))) or None
        rating_hint = _parse_rating_hint(fields.get("ОЦЕНКА В UNTAPPD"))
        listings.append(
            GlideListing(
                name=name,
                brewery=brewery,
                style=style,
                untappd_url=untappd_url,
                rating_hint=rating_hint,
                alc=_extract_alc_text(fields),
                flavor_notes=_extract_flavor_notes(fields),
            )
        )

    return listings


def parse_untappd_beer_page(html: str) -> UntappdBeerPage:
    rating: float | None = None
    rating_count: int | None = None
    abv: float | None = None
    ibu: int | None = None

    for raw_json in _UNTAPPD_JSON_LD_RE.findall(html):
        try:
            payload = json.loads(unescape(raw_json).strip())
        except json.JSONDecodeError:
            continue

        aggregate_rating = _find_aggregate_rating(payload)
        if not aggregate_rating:
            continue

        rating_value = aggregate_rating.get("ratingValue")
        review_count = aggregate_rating.get("reviewCount") or aggregate_rating.get("ratingCount")
        if rating_value is not None and review_count is not None:
            try:
                rating = float(str(rating_value))
                rating_count = int(str(review_count).replace(",", ""))
            except (TypeError, ValueError):
                continue
            break

    if rating is None or rating_count is None:
        meta_match = _UNTAPPD_META_RE.search(html)
        if meta_match:
            rating = float(meta_match.group(1))
            rating_count = int(meta_match.group(2).replace(",", ""))

    if rating is None or rating_count is None:
        raise ValueError("Untappd beer page is missing rating metadata")

    abv_match = _UNTAPPD_ABV_RE.search(html)
    if abv_match:
        abv = float(abv_match.group(1))

    ibu_match = _UNTAPPD_IBU_RE.search(html)
    if ibu_match:
        ibu = int(ibu_match.group(1))

    return UntappdBeerPage(rating=rating, rating_count=rating_count, abv=abv, ibu=ibu)


def format_beer_message(grouped: dict[str, list[BeerEntry]]) -> str:
    lines = ["Смотри какое интересное пиво я нашел:"]

    for category in CATEGORY_ORDER:
        beers = grouped.get(category, [])
        if not beers:
            continue

        lines.append("")
        emoji = CATEGORY_EMOJI.get(category)
        label = category
        if emoji:
            label = f"{emoji}{emoji}{emoji} {category} {emoji}{emoji}{emoji}"
        lines.append(f"<b>{escape(label)}</b>")

        for beer in beers[:5]:
            header = f"• {escape(beer.name)}"
            if beer.flavor_notes:
                header = f"{header} ({escape(beer.flavor_notes)})"
            brewery = _strip_city_suffix(beer.brewery)
            if brewery:
                header = f"{header} - {escape(brewery)}"
            lines.append(header)
            lines.append(_format_beer_stat_line(beer))
            lines.append("")

        if lines[-1] == "":
            lines.pop()

    return "\n".join(lines)


def parse_search_query(text: str) -> BeerSearchQuery:
    normalized = _normalize_text(text)
    categories: list[str] = []
    exclude_categories: list[str] = []

    category_aliases = (
        ("New England IPA", ("new england ipa", "new england", "ne ipa", "neipa", "hazy ipa")),
        ("IPA", (" ipa ", "ipa", "american ipa", "west coast ipa")),
        ("Pastry Sour Ale", ("pastry sour", "smoothie sour", "pastry")),
        ("Sour Ale", ("sour ale", "sour", "gose")),
        ("Weizen", ("weizen", "hefeweizen", "wheat", "witbier", "white ale")),
        ("Безалкогольное", ("безал", "безалкоголь", "non alco", "non alcohol", "non alcoholic")),
    )
    padded = f" {normalized} "
    for category, aliases in category_aliases:
        if any(f" {_normalize_text(alias)} " in padded for alias in aliases):
            categories.append(category)

    negative_aliases = (
        (("new england ipa", "new england", "ne ipa", "neipa", "hazy ipa"), ("New England IPA",)),
        (("ipa", "american ipa", "west coast ipa"), ("IPA", "New England IPA")),
        (("pastry sour", "smoothie sour", "pastry"), ("Pastry Sour Ale",)),
        (("sour ale", "sour", "gose"), ("Sour Ale", "Pastry Sour Ale")),
        (("weizen", "hefeweizen", "wheat", "witbier", "white ale"), ("Weizen",)),
        (("безал", "безалкоголь", "non alco", "non alcohol", "non alcoholic"), ("Безалкогольное",)),
    )
    for aliases, excluded in negative_aliases:
        for alias in aliases:
            normalized_alias = _normalize_text(alias)
            if re.search(rf"\bне\s+{re.escape(normalized_alias)}\b", normalized):
                exclude_categories.extend(excluded)

    max_alc: float | None = None
    alc_match = re.search(
        r"(?:до|не крепче|не больше|не выше|меньше|ниже|макс(?:имум)?|maximum|max)\s*(\d+(?:[.,]\d+)?)",
        text,
        re.IGNORECASE,
    )
    if alc_match:
        max_alc = float(alc_match.group(1).replace(",", "."))

    min_rating: float | None = None
    rating_match = re.search(
        r"(?:от|>=?|выше|больше)\s*(\d(?:[.,]\d+)?)\s*(?:рейтинга|рейтинг)?",
        text,
        re.IGNORECASE,
    )
    if rating_match:
        min_rating = float(rating_match.group(1).replace(",", "."))
    elif re.search(r"высок\w*\s+рейтинг|топ|top rating", text, re.IGNORECASE):
        min_rating = 4.0

    stop_words = {
        "beer", "ale", "ipa", "sour", "pastry", "new", "england", "ne", "hazy",
        "weizen", "wheat", "non", "alco", "alcohol", "безал", "безалкогольное",
        "до", "не", "крепче", "максимум", "max", "maximum", "от", "рейтинг",
        "рейтинга", "рейтингом",
        "выше", "больше", "меньше", "ниже", "найди", "найти", "подбери", "подобрать",
        "выбери", "выбрать", "покажи",
        "высоким", "высокий", "высокого", "с", "и", "на", "по", "пиво",
        "градусов", "градуса", "градус", "алкоголя", "алкоголем",
    }
    tokens = tuple(
        token
        for token in _normalize_text(text).split()
        if len(token) > 1 and not re.fullmatch(r"\d+(?:[.,]\d+)?", token) and token not in stop_words
    )

    return BeerSearchQuery(
        raw_text=text.strip(),
        categories=tuple(dict.fromkeys(categories)),
        exclude_categories=tuple(dict.fromkeys(exclude_categories)),
        max_alc=max_alc,
        min_rating=min_rating,
        tokens=tokens,
    )


def merge_search_queries(primary: BeerSearchQuery, secondary: BeerSearchQuery) -> BeerSearchQuery:
    return BeerSearchQuery(
        raw_text=primary.raw_text or secondary.raw_text,
        categories=tuple(dict.fromkeys((*secondary.categories, *primary.categories))),
        exclude_categories=tuple(
            dict.fromkeys((*secondary.exclude_categories, *primary.exclude_categories))
        ),
        max_alc=secondary.max_alc if secondary.max_alc is not None else primary.max_alc,
        min_rating=secondary.min_rating if secondary.min_rating is not None else primary.min_rating,
        tokens=tuple(dict.fromkeys((*primary.tokens, *secondary.tokens))),
    )


def format_beer_search_message(query: BeerSearchQuery, beers: list[BeerEntry], *, fallback: bool) -> str:
    intro = (
        f'Точного совпадения по запросу "{escape(query.raw_text)}" не нашел, вот самые близкие варианты:'
        if fallback
        else f'Вот что нашел по запросу "{escape(query.raw_text)}":'
    )
    lines = [intro]

    for beer in beers[:5]:
        header = f"• {escape(beer.name)}"
        if beer.flavor_notes:
            header = f"{header} ({escape(beer.flavor_notes)})"
        brewery = _strip_city_suffix(beer.brewery)
        if brewery:
            header = f"{header} - {escape(brewery)}"
        lines.append("")
        lines.append(header)
        lines.append(_format_beer_stat_line(beer))

    return "\n".join(lines)


def format_no_beer_search_matches(query: BeerSearchQuery) -> str:
    if query.categories:
        categories = ", ".join(query.categories)
        return (
            f'По запросу "{escape(query.raw_text)}" не нашел подходящих вариантов в наличии. '
            f"Сейчас нет подходящего пива в категории: {escape(categories)}."
        )
    return f'По запросу "{escape(query.raw_text)}" не нашел подходящих вариантов в наличии.'


def rank_category_entries(entries: list[BeerEntry]) -> dict[str, list[BeerEntry]]:
    grouped: dict[str, list[BeerEntry]] = {category: [] for category in CATEGORY_ORDER}

    for entry in entries:
        category = categorize_style(entry.style, entry.alc)
        if category is None:
            continue
        grouped[category].append(entry)

    ranked: dict[str, list[BeerEntry]] = {}
    for category in CATEGORY_ORDER:
        category_entries = grouped[category]
        if not category_entries:
            continue
        ranked[category] = sorted(
            category_entries,
            key=lambda entry: (
                -weighted_score(entry),
                -entry.rating_count,
                -entry.rating,
                entry.name,
            ),
        )

    return ranked


class BeerTopService:
    def __init__(
        self,
        *,
        cache_ttl: timedelta = timedelta(hours=6),
        request_timeout: float = 15.0,
        cache_path: Path | None = None,
        max_cache_bytes: int = MAX_CACHE_BYTES,
    ) -> None:
        self._cache_ttl = cache_ttl
        self._request_timeout = request_timeout
        self._cache_path = cache_path or CACHE_PATH
        self._max_cache_bytes = max_cache_bytes
        self._cache_text: str | None = None
        self._cache_until: datetime | None = None
        self._cache_entries: list[BeerEntry] | None = None
        self._groq_api_key = os.getenv("GROQ_API_KEY")
        self._groq_model = os.getenv("GROQ_MODEL", "openai/gpt-oss-20b")
        self._openai_client = (
            OpenAI(api_key=self._groq_api_key, base_url="https://api.groq.com/openai/v1")
            if OpenAI is not None and self._groq_api_key
            else None
        )

    async def build_message(self) -> str | None:
        if self._cache_text and self._cache_until and datetime.now(UTC) < self._cache_until:
            return self._cache_text

        entries = self.load_cached_entries()
        if not entries:
            return None

        grouped = rank_category_entries(entries)
        if not grouped:
            return None

        text = format_beer_message(grouped)
        self._cache_text = text
        self._cache_until = datetime.now(UTC) + self._cache_ttl
        return text

    async def search_message(self, query_text: str) -> str | None:
        query = await self.parse_user_query(query_text)
        if not query.raw_text:
            return None

        entries = self.load_cached_inventory()
        if not entries:
            return None

        exact_matches = self.search_entries(entries, query)
        if exact_matches:
            reranked = await self.rerank_candidates_with_llm(query.raw_text, exact_matches[:12])
            final_matches = reranked or exact_matches
            return format_beer_search_message(query, final_matches, fallback=False)

        closest_matches = self.closest_matches(entries, query)
        if not closest_matches:
            return format_no_beer_search_matches(query)
        reranked = await self.rerank_candidates_with_llm(query.raw_text, closest_matches[:12])
        final_matches = reranked or closest_matches
        return format_beer_search_message(query, final_matches, fallback=True)

    async def parse_user_query(self, query_text: str) -> BeerSearchQuery:
        parsed = parse_search_query(query_text)
        llm_query = await self.parse_query_with_llm(query_text)
        if llm_query is None:
            return parsed
        return merge_search_queries(parsed, llm_query)

    async def parse_query_with_llm(self, query_text: str) -> BeerSearchQuery | None:
        if self._openai_client is None:
            return None

        try:
            llm_result = await asyncio.to_thread(self._parse_query_with_llm_sync, query_text)
        except Exception as exc:
            LOGGER.warning("LLM beer query parse failed: %s", exc)
            return None

        if llm_result is None:
            return None

        categories = tuple(category for category in llm_result.categories if category in CATEGORY_ORDER)
        exclude_categories = tuple(
            category for category in llm_result.exclude_categories if category in CATEGORY_ORDER
        )
        tokens = tuple(
            dict.fromkeys(
                _normalize_text(token)
                for token in (*llm_result.flavor_tokens, *llm_result.hop_tokens)
                if _normalize_text(token)
            )
        )
        return BeerSearchQuery(
            raw_text=query_text.strip(),
            categories=categories,
            exclude_categories=exclude_categories,
            max_alc=llm_result.max_alc,
            min_rating=llm_result.min_rating,
            tokens=tokens,
        )

    def _parse_query_with_llm_sync(self, query_text: str) -> _LLMBeerSearchQuery | None:
        if self._openai_client is None:
            return None

        response = self._openai_client.responses.parse(
            model=self._groq_model,
            input=[
                {
                    "role": "system",
                    "content": (
                        "You extract beer search filters from Russian or English user requests. "
                        "Return only structured data. Use only these categories when appropriate: "
                        "New England IPA, IPA, Pastry Sour Ale, Sour Ale, Weizen, Безалкогольное. "
                        "Put hop names and flavor descriptors into hop_tokens and flavor_tokens. "
                        "If the user asks for high rating, set min_rating to 4.0 unless a stricter value is given."
                    ),
                },
                {"role": "user", "content": query_text},
            ],
            text_format=_LLMBeerSearchQuery,
        )
        return response.output_parsed

    async def rerank_candidates_with_llm(
        self,
        query_text: str,
        candidates: list[BeerEntry],
    ) -> list[BeerEntry] | None:
        if self._openai_client is None or not candidates:
            return None

        try:
            reranked_ids = await asyncio.to_thread(
                self._rerank_candidates_with_llm_sync,
                query_text,
                candidates,
            )
        except Exception as exc:
            LOGGER.warning("LLM beer rerank failed: %s", exc)
            return None

        if not reranked_ids:
            return None

        by_id = {index: entry for index, entry in enumerate(candidates, start=1)}
        reranked = [by_id[item_id] for item_id in reranked_ids if item_id in by_id]
        return reranked or None

    def _rerank_candidates_with_llm_sync(
        self,
        query_text: str,
        candidates: list[BeerEntry],
    ) -> list[int]:
        candidate_payload = [
            {
                "id": index,
                "name": candidate.name,
                "brewery": candidate.brewery,
                "style": candidate.style,
                "alc": candidate.alc,
                "rating": candidate.rating,
                "rating_count": candidate.rating_count,
                "flavor_notes": candidate.flavor_notes,
            }
            for index, candidate in enumerate(candidates, start=1)
        ]
        response = self._openai_client.responses.parse(
            model=self._groq_model,
            input=[
                {
                    "role": "system",
                    "content": (
                        "You rank beer candidates for a user query. "
                        "Select only from the provided candidates. "
                        "Prefer candidates that best match the user's vibe and constraints. "
                        "Return selected_ids ordered best to worst."
                    ),
                },
                {
                    "role": "user",
                    "content": json.dumps(
                        {
                            "query": query_text,
                            "candidates": candidate_payload,
                        },
                        ensure_ascii=False,
                    ),
                },
            ],
            text_format=_LLMBeerRerankResult,
        )
        parsed = response.output_parsed
        if parsed is None:
            return []
        return parsed.selected_ids

    async def fetch_ranked_entries(self) -> list[BeerEntry]:
        if self._cache_entries and self._cache_until and datetime.now(UTC) < self._cache_until:
            return self._cache_entries

        payload = self._load_cache_payload()
        if payload is None:
            return []
        entries = self._deserialize_entries(payload.get("entries"))
        self._cache_entries = entries
        self._cache_until = datetime.now(UTC) + self._cache_ttl
        return entries

    async def refresh_cache(self) -> int:
        entries, glide_url = await self.fetch_live_entries()
        self._write_cache(entries, glide_url)
        self._cache_entries = entries
        self._cache_text = None
        self._cache_until = datetime.now(UTC) + self._cache_ttl
        return len(entries)

    def load_cached_entries(self) -> list[BeerEntry]:
        if self._cache_entries and self._cache_until and datetime.now(UTC) < self._cache_until:
            return [entry for entry in self._cache_entries if entry.rating_available]

        payload = self._load_cache_payload()
        if payload is None:
            return []
        entries = self._deserialize_entries(payload.get("ranked_entries"))
        self._cache_entries = entries
        self._cache_until = datetime.now(UTC) + self._cache_ttl
        return entries

    def load_cached_inventory(self) -> list[BeerEntry]:
        payload = self._load_cache_payload()
        if payload is None:
            return []
        inventory = self._deserialize_entries(payload.get("inventory"))
        return inventory

    async def fetch_live_entries(self) -> tuple[list[BeerEntry], str | None]:
        channel_html = await self.fetch_channel_html()
        glide_url = extract_latest_glide_url(channel_html)
        if not glide_url:
            return [], None

        glide_html = await self.fetch_glide_html(glide_url)
        listings = parse_glide_listings(glide_html)
        if not listings:
            app_id = extract_glide_app_id(glide_url)
            if app_id:
                listings = await self.fetch_firestore_inventory(app_id)
        if not listings:
            return [], glide_url

        entries = await self.resolve_untappd_matches(listings)
        return entries, glide_url

    def search_entries(self, entries: list[BeerEntry], query: BeerSearchQuery) -> list[BeerEntry]:
        matches: list[BeerEntry] = []
        for entry in entries:
            if not _entry_matches_query(entry, query):
                continue
            matches.append(entry)

        return sorted(
            matches,
            key=lambda entry: (
                -_entry_search_score(entry, query, exact=True),
                -weighted_score(entry),
                entry.name,
            ),
        )

    def closest_matches(self, entries: list[BeerEntry], query: BeerSearchQuery) -> list[BeerEntry]:
        scored = [
            (entry, _entry_search_score(entry, query, exact=False))
            for entry in entries
        ]
        scored = [item for item in scored if item[1] > 0]
        flavor_tokens = [token for token in query.tokens if token in FLAVOR_HINT_TOKENS]
        if flavor_tokens:
            flavor_matched = [
                item
                for item in scored
                if all(token in _entry_search_blob(item[0]) for token in flavor_tokens)
            ]
            if flavor_matched:
                scored = flavor_matched
        if query.categories:
            category_scored = [
                item
                for item in scored
                if categorize_style(item[0].style, item[0].alc) in query.categories
            ]
            if category_scored:
                scored = category_scored
            else:
                return []
        allowed_scored = [
            item
            for item in scored
            if categorize_style(item[0].style, item[0].alc) not in query.exclude_categories
        ]
        if allowed_scored:
            scored = allowed_scored
        scored.sort(key=lambda item: (-item[1], -weighted_score(item[0]), item[0].name))
        return [entry for entry, _ in scored[:5]]

    async def fetch_firestore_inventory(
        self,
        app_id: str,
        *,
        prioritize_for_enrichment: bool = False,
    ) -> list[GlideListing]:
        document_json = await self.fetch_published_data_document(app_id)
        table_doc_id = extract_inventory_table_doc_id(document_json)
        if table_doc_id is None:
            return []

        pages = await self.fetch_table_rows_pages(app_id, table_doc_id)
        listings: list[GlideListing] = []
        seen: set[tuple[str, str]] = set()
        for page in pages:
            for listing in parse_firestore_inventory_rows(page):
                key = (_normalize_text(listing.name), _normalize_text(listing.brewery or ""))
                if key in seen:
                    continue
                seen.add(key)
                listings.append(listing)
        if prioritize_for_enrichment:
            return _prioritize_direct_untappd_candidates(listings)
        return listings

    async def resolve_untappd_matches(self, listings: list[GlideListing]) -> list[BeerEntry]:
        semaphore = asyncio.Semaphore(8)

        async def enrich_listing(listing: GlideListing) -> BeerEntry | None:
            try:
                if listing.untappd_url and listing.style:
                    async with semaphore:
                        page_html = await self.fetch_untappd_beer_page_html(listing.untappd_url)
                    details = parse_untappd_beer_page(page_html)
                    return BeerEntry(
                        name=listing.name,
                        brewery=listing.brewery,
                        style=listing.style,
                        rating=details.rating,
                        rating_count=details.rating_count,
                        alc=listing.alc,
                        flavor_notes=listing.flavor_notes,
                        untappd_url=listing.untappd_url,
                        rating_available=True,
                        untappd_abv=details.abv,
                        untappd_ibu=details.ibu,
                    )

                query = listing.name
                if listing.brewery:
                    query = f"{listing.name} {listing.brewery}"
                async with semaphore:
                    search_html = await self.fetch_untappd_search_html(query)
                match = select_best_untappd_match(listing, parse_untappd_search_results(search_html))
                if match is None:
                    return None

                async with semaphore:
                    page_html = await self.fetch_untappd_beer_page_html(match.url)
                details = parse_untappd_beer_page(page_html)
            except Exception as exc:
                LOGGER.warning("Beer enrichment failed for %s: %s", listing.name, exc)
                if listing.style is None:
                    return None
                return BeerEntry(
                    name=listing.name,
                    brewery=listing.brewery,
                    style=listing.style,
                    rating=float(listing.rating_hint or 0.0),
                    rating_count=0,
                    alc=listing.alc,
                    flavor_notes=listing.flavor_notes,
                    untappd_url=listing.untappd_url,
                    rating_available=False,
                    untappd_abv=None,
                    untappd_ibu=None,
                )

            return BeerEntry(
                name=match.name,
                brewery=match.brewery,
                style=match.style,
                rating=details.rating,
                rating_count=details.rating_count,
                alc=listing.alc,
                flavor_notes=listing.flavor_notes,
                untappd_url=match.url,
                rating_available=True,
                untappd_abv=details.abv,
                untappd_ibu=details.ibu,
            )

        results = await asyncio.gather(*(enrich_listing(listing) for listing in listings))
        entries = [entry for entry in results if entry is not None]
        return entries

    async def fetch_channel_html(self) -> str:
        return await self._fetch_text(CHANNEL_URL)

    async def fetch_glide_html(self, glide_url: str) -> str:
        return await self._fetch_text(glide_url)

    async def fetch_published_data_document(self, app_id: str) -> str:
        return await self._fetch_text(GLIDE_PUBLISHED_DATA_URL.format(app_id=quote(app_id, safe="")))

    async def fetch_table_rows_pages(self, app_id: str, table_doc_id: str) -> list[str]:
        pages: list[str] = []
        page_token: str | None = None

        while True:
            url = GLIDE_TABLE_ROWS_URL.format(
                app_id=quote(app_id, safe=""),
                table_doc_id=quote(table_doc_id, safe=""),
            )
            if page_token:
                url = f"{url}?{urlencode({'pageToken': page_token})}"

            page = await self._fetch_text(url)
            pages.append(page)

            payload = json.loads(page)
            next_page_token = payload.get("nextPageToken")
            if not isinstance(next_page_token, str) or not next_page_token:
                break
            page_token = next_page_token

        return pages

    async def fetch_untappd_search_html(self, query: str) -> str:
        return await self._fetch_text(UNTAPPD_SEARCH_URL.format(query=quote_plus(query)))

    async def fetch_untappd_beer_page_html(self, url: str) -> str:
        return await self._fetch_text(url)

    async def _fetch_text(self, url: str) -> str:
        return await asyncio.to_thread(self._fetch_text_sync, url)

    def _fetch_text_sync(self, url: str) -> str:
        request = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(request, timeout=self._request_timeout) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            return response.read().decode(charset, errors="replace")

    def _load_cache_payload(self) -> dict[str, object] | None:
        if not self._cache_path.exists():
            return None
        try:
            payload = json.loads(self._cache_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            LOGGER.warning("Failed to read beer cache: %s", exc)
            return None
        if not isinstance(payload, dict):
            return None
        return payload

    def _deserialize_entries(self, raw_entries: object) -> list[BeerEntry]:
        if not isinstance(raw_entries, list):
            return []
        entries: list[BeerEntry] = []
        for item in raw_entries:
            if not isinstance(item, dict):
                continue
            try:
                entries.append(
                    BeerEntry(
                        name=str(item["name"]),
                        brewery=item.get("brewery"),
                        style=str(item["style"]),
                        rating=float(item["rating"]),
                        rating_count=int(item["rating_count"]),
                        alc=item.get("alc"),
                        flavor_notes=item.get("flavor_notes"),
                        untappd_url=item.get("untappd_url"),
                        rating_available=bool(item.get("rating_available", True)),
                        untappd_abv=(
                            float(item["untappd_abv"])
                            if item.get("untappd_abv") is not None
                            else None
                        ),
                        untappd_ibu=(
                            int(item["untappd_ibu"])
                            if item.get("untappd_ibu") is not None
                            else None
                        ),
                    )
                )
            except (KeyError, TypeError, ValueError):
                continue
        return entries

    def _write_cache(self, entries: list[BeerEntry], glide_url: str | None) -> None:
        payload = {
            "refreshed_at": datetime.now(UTC).isoformat(),
            "source_glide_url": glide_url,
            "inventory": [asdict(entry) for entry in entries],
            "ranked_entries": [asdict(entry) for entry in entries if entry.rating_available],
        }
        serialized = json.dumps(payload, ensure_ascii=False, indent=2)
        encoded = serialized.encode("utf-8")
        if len(encoded) > self._max_cache_bytes:
            raise ValueError("Beer cache exceeds configured size limit")

        self._cache_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self._cache_path.with_suffix(".tmp")
        temp_path.write_bytes(encoded)
        temp_path.replace(self._cache_path)


def select_best_untappd_match(
    listing: GlideListing, results: list[UntappdSearchResult]
) -> UntappdSearchResult | None:
    best_match: UntappdSearchResult | None = None
    best_score = 0.0

    listing_name = _normalize_text(listing.name)
    listing_tokens = _meaningful_tokens(listing.name)
    brewery_name = _normalize_text(listing.brewery or "")
    listing_brewery_tokens = _meaningful_brewery_tokens(listing.brewery or "")

    for result in results:
        result_name = _normalize_text(result.name)
        name_similarity = SequenceMatcher(None, listing_name, result_name).ratio()
        overlap = len(listing_tokens & _meaningful_tokens(result.name))
        if overlap == 0 and name_similarity < 0.7:
            continue

        brewery_similarity = 0.5
        if brewery_name:
            result_brewery = _normalize_text(result.brewery or "")
            result_brewery_tokens = _meaningful_brewery_tokens(result.brewery or "")
            brewery_similarity = SequenceMatcher(None, brewery_name, result_brewery).ratio()

            if (
                result.brewery
                and listing_brewery_tokens
                and name_similarity >= 0.9
                and brewery_similarity < 0.9
                and not _brewery_tokens_compatible(
                    listing_brewery_tokens,
                    result_brewery_tokens,
                )
            ):
                continue

        score = (name_similarity * 0.65) + (brewery_similarity * 0.35) + (overlap * 0.05)
        if score > best_score:
            best_score = score
            best_match = result

    if best_score < 0.75:
        return None

    return best_match


def _find_glide_listings(payload: object) -> list[GlideListing]:
    listings: list[GlideListing] = []
    seen: set[tuple[str, str]] = set()

    def visit(node: object) -> None:
        if isinstance(node, dict):
            listing = _listing_from_mapping(node)
            if listing is not None:
                key = (_normalize_text(listing.name), _normalize_text(listing.brewery or ""))
                if key not in seen:
                    seen.add(key)
                    listings.append(listing)
            for value in node.values():
                visit(value)
            return

        if isinstance(node, list):
            for item in node:
                visit(item)

    visit(payload)
    return listings


def _listing_from_mapping(mapping: dict[object, object]) -> GlideListing | None:
    has_strong_listing_context = any(
        key in mapping
        for key in (
            "brewery",
            "breweryName",
            "brand",
            "available",
            "status",
            "beerName",
            "style",
            "abv",
            "price",
            "size",
        )
    )
    name_value = mapping.get("beerName")
    if name_value is None and has_strong_listing_context:
        name_value = mapping.get("name")
    if name_value is None and has_strong_listing_context:
        name_value = mapping.get("title")
    brewery_value = mapping.get("brewery") or mapping.get("breweryName") or mapping.get("brand")
    available = mapping.get("available")
    status = mapping.get("status")

    if name_value is None:
        return None

    if available is False:
        return None

    if isinstance(status, str) and status.lower() not in {"available", "in_stock", "in stock"}:
        return None

    name = _clean_text(str(name_value))
    if not name:
        return None

    brewery = _clean_text(str(brewery_value)) or None if brewery_value is not None else None
    return GlideListing(name=name, brewery=brewery)


def _find_aggregate_rating(payload: object) -> dict[str, object] | None:
    if isinstance(payload, dict):
        aggregate_rating = payload.get("aggregateRating")
        if isinstance(aggregate_rating, dict):
            return aggregate_rating

        for value in payload.values():
            found = _find_aggregate_rating(value)
            if found is not None:
                return found

    if isinstance(payload, list):
        for item in payload:
            found = _find_aggregate_rating(item)
            if found is not None:
                return found

    return None


def _decode_published_schema(value: str) -> object:
    try:
        raw = value.encode("latin1")
        return json.loads(gzip.decompress(raw).decode("utf-8"))
    except (UnicodeEncodeError, OSError, json.JSONDecodeError):
        return json.loads(value)


def _encode_table_doc_id(table_name: object) -> str | None:
    if not isinstance(table_name, dict):
        return None

    raw_name = table_name.get("name")
    if not isinstance(raw_name, str) or not raw_name:
        return None

    if table_name.get("isSpecial") is True:
        return f"${raw_name}"

    encoded = "_" + "".join(
        "#_" if char == "/" else "##" if char == "#" else char
        for char in raw_name
    )
    if len(encoded) > 1500:
        encoded = hashlib.md5(encoded.encode("utf-8")).hexdigest()
    return encoded


def _decode_firestore_fields(fields: object) -> dict[str, object]:
    if not isinstance(fields, dict):
        return {}
    return {
        key: _decode_firestore_value(value)
        for key, value in fields.items()
        if isinstance(key, str)
    }


def _decode_firestore_value(value: object) -> object:
    if not isinstance(value, dict):
        return value

    if "stringValue" in value:
        return value["stringValue"]
    if "integerValue" in value:
        return int(value["integerValue"])
    if "doubleValue" in value:
        return float(value["doubleValue"])
    if "booleanValue" in value:
        return bool(value["booleanValue"])
    if "nullValue" in value:
        return None
    if "timestampValue" in value:
        return value["timestampValue"]
    if "mapValue" in value:
        return _decode_firestore_fields(value["mapValue"].get("fields", {}))
    if "arrayValue" in value:
        values = value["arrayValue"].get("values", [])
        return [_decode_firestore_value(item) for item in values]

    return value


def _parse_rating_hint(value: object) -> float | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().replace(",", ".")
    try:
        return float(normalized)
    except ValueError:
        return None


def _parse_alc_value(value: str | None) -> float | None:
    if not value:
        return None

    first_chunk = value.split("/", 1)[0].strip().replace(",", ".")
    try:
        return float(first_chunk)
    except ValueError:
        return None


def _parse_glide_ibu_value(value: str | None) -> int | None:
    if not value:
        return None
    parts = value.split("/")
    if len(parts) < 2:
        return None
    candidate = parts[1].strip().replace(",", ".")
    if not candidate or candidate == "-":
        return None
    try:
        return int(float(candidate))
    except ValueError:
        return None


def _format_beer_stat_line(entry: BeerEntry) -> str:
    abv = entry.untappd_abv
    ibu = entry.untappd_ibu

    if abv is None:
        abv = _parse_alc_value(entry.alc)
    if ibu is None:
        ibu = _parse_glide_ibu_value(entry.alc)

    abv_text = f"{abv:.1f}%" if abv is not None else "-"
    ibu_text = f"{ibu} IBU" if ibu is not None else "-"
    rating_text = f"{entry.rating:.2f}" if entry.rating_available else "-"
    rating_count_text = f"{entry.rating_count:,}" if entry.rating_available else "-"
    return f"🥃 {abv_text} | 🌲 {ibu_text} | ⭐ {rating_text} | 👥 {rating_count_text}"


def _extract_alc_text(fields: dict[str, object]) -> str | None:
    known_keys = {
        "НАЗВАНИЕ",
        "ПИВОВАРНЯ",
        "СТИЛЬ",
        "ДОСТУПНО В БАРЕ",
        "ОТКРЫТЬ В UNTAPPD",
        "ОЦЕНКА В UNTAPPD",
        "UNTAPPD ICON",
        "ИЗОБРАЖЕНИЕ",
        "ОПИСАНИЕ",
        "ЦЕНА",
        "Объём",
        "$rowIndex",
        "$rowVersion",
    }
    for key, value in fields.items():
        if key in known_keys or not isinstance(value, str):
            continue
        if re.fullmatch(r"[\d.,]+/[-\d.,]+/[-\d.,]+", value.strip()):
            return value.strip()
    return None


def _extract_flavor_notes(fields: dict[str, object]) -> str | None:
    description = fields.get("ОПИСАНИЕ")
    if not isinstance(description, str):
        description = ""

    cleaned = _clean_text(description)
    if cleaned:
        return cleaned

    style = fields.get("СТИЛЬ")
    if not isinstance(style, str):
        return None

    match = re.search(r"\(([^()]+)\)\s*$", style)
    if match is None:
        return None

    notes = _clean_text(match.group(1))
    return notes or None


def _entry_search_blob(entry: BeerEntry) -> str:
    parts = [
        entry.name,
        entry.brewery or "",
        entry.style,
        entry.flavor_notes or "",
    ]
    return _normalize_text(" ".join(parts))


def _entry_matches_query(entry: BeerEntry, query: BeerSearchQuery) -> bool:
    category = categorize_style(entry.style, entry.alc)
    if query.categories and category not in query.categories:
        return False
    if query.exclude_categories and category in query.exclude_categories:
        return False

    alc_value = _parse_alc_value(entry.alc)
    if query.max_alc is not None and (alc_value is None or alc_value > query.max_alc):
        return False

    if query.min_rating is not None and entry.rating < query.min_rating:
        return False

    searchable = _entry_search_blob(entry)
    return all(token in searchable for token in query.tokens)


def _entry_search_score(entry: BeerEntry, query: BeerSearchQuery, *, exact: bool) -> float:
    score = weighted_score(entry)
    searchable = _entry_search_blob(entry)
    category = categorize_style(entry.style, entry.alc)

    if query.categories:
        score += 3.0 if category in query.categories else (-1.0 if exact else 0.0)
    if query.exclude_categories and category in query.exclude_categories:
        score += -4.0 if exact else -2.5
    if query.min_rating is not None:
        score += 2.0 if entry.rating >= query.min_rating else (-1.5 if exact else -0.25)

    alc_value = _parse_alc_value(entry.alc)
    if query.max_alc is not None:
        if alc_value is None:
            score += -1.0 if exact else 0.0
        elif alc_value <= query.max_alc:
            score += 2.0
        else:
            score += -2.0 if exact else max(-1.5, (query.max_alc - alc_value) * 0.3)

    for token in query.tokens:
        if token in searchable:
            score += 2.5
        elif not exact:
            similarity = max(
                (SequenceMatcher(None, token, word).ratio() for word in searchable.split()),
                default=0.0,
            )
            if similarity >= 0.75:
                score += 1.0

    return score


def _prioritize_direct_untappd_candidates(
    listings: list[GlideListing],
    *,
    per_category_limit: int = 12,
) -> list[GlideListing]:
    direct_candidates: dict[str, list[GlideListing]] = {category: [] for category in CATEGORY_ORDER}
    fallback_listings: list[GlideListing] = []

    for listing in listings:
        if not listing.untappd_url or not listing.style:
            fallback_listings.append(listing)
            continue
        category = categorize_style(listing.style, listing.alc)
        if category is None:
            continue
        direct_candidates[category].append(listing)

    prioritized: list[GlideListing] = []
    seen: set[tuple[str, str]] = set()

    for category in CATEGORY_ORDER:
        category_listings = sorted(
            direct_candidates[category],
            key=lambda listing: (
                -(listing.rating_hint if listing.rating_hint is not None else -1.0),
                listing.name,
            ),
        )
        for listing in category_listings[:per_category_limit]:
            key = (_normalize_text(listing.name), _normalize_text(listing.brewery or ""))
            if key in seen:
                continue
            seen.add(key)
            prioritized.append(listing)

    for listing in fallback_listings:
        key = (_normalize_text(listing.name), _normalize_text(listing.brewery or ""))
        if key in seen:
            continue
        seen.add(key)
        prioritized.append(listing)

    return prioritized


def _strip_city_suffix(brewery: str | None) -> str | None:
    if brewery is None:
        return None
    cleaned = re.sub(r"\s*\((?:г\.?|г)\s*[^)]*\)\s*$", "", brewery).strip()
    return cleaned or None


def _clean_text(value: str) -> str:
    return " ".join(_TAG_RE.sub(" ", unescape(value)).split())


def _normalize_text(value: str) -> str:
    normalized = _clean_text(value).casefold()
    return re.sub(r"[^a-z0-9а-яё]+", " ", normalized).strip()


def _meaningful_tokens(value: str) -> set[str]:
    tokens = {
        token
        for token in _normalize_text(value).split()
        if len(token) > 2 and token not in {"the", "and", "beer", "ale"}
    }
    return tokens


def _meaningful_brewery_tokens(value: str) -> set[str]:
    return {
        token
        for token in _normalize_text(value).split()
        if token
        not in {
            "brewery",
            "brewing",
            "company",
            "co",
            "beer",
            "project",
            "house",
        }
    }


def _brewery_tokens_compatible(left: set[str], right: set[str]) -> bool:
    if not left or not right:
        return True

    if left <= right or right <= left:
        return True

    return False
