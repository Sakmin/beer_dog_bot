from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from difflib import SequenceMatcher
import json
import logging
import math
import re
from html import unescape
from html.parser import HTMLParser
from urllib.parse import quote_plus, urljoin
from urllib.request import Request, urlopen


CATEGORY_ORDER = (
    "New England IPA",
    "IPA",
    "Sour Ale",
    "Pastry Sour Ale",
    "Безалкогольное",
)

LOGGER = logging.getLogger(__name__)
CHANNEL_URL = "https://t.me/s/beerhounds73"
UNTAPPD_SEARCH_URL = "https://untappd.com/search?q={query}"


@dataclass(slots=True)
class BeerEntry:
    name: str
    brewery: str | None
    style: str
    rating: float
    rating_count: int


@dataclass(slots=True)
class GlideListing:
    name: str
    brewery: str | None = None


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


def categorize_style(style: str) -> str | None:
    normalized = style.lower()

    if any(
        marker in normalized
        for marker in (
            "безалкоголь",
            "non-alcohol",
            "non alcoholic",
            "alcohol-free",
            "alcohol free",
            "0.0",
            "0%",
        )
    ):
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


def parse_untappd_beer_page(html: str) -> UntappdBeerPage:
    rating: float | None = None
    rating_count: int | None = None

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

    return UntappdBeerPage(rating=rating, rating_count=rating_count)


def format_beer_message(grouped: dict[str, list[BeerEntry]]) -> str:
    lines = ["Смотри какое интересное пиво я нашел:"]

    for category in CATEGORY_ORDER:
        beers = grouped.get(category, [])
        if not beers:
            continue

        lines.append("")
        lines.append(category)

        for beer in beers[:5]:
            header = beer.name
            if beer.brewery:
                header = f"{header} - {beer.brewery}"
            lines.append(
                f"{header} | Untappd {beer.rating:.2f} | {beer.rating_count:,} ratings"
            )

    return "\n".join(lines)


def rank_category_entries(entries: list[BeerEntry]) -> dict[str, list[BeerEntry]]:
    grouped: dict[str, list[BeerEntry]] = {category: [] for category in CATEGORY_ORDER}

    for entry in entries:
        category = categorize_style(entry.style)
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
    ) -> None:
        self._cache_ttl = cache_ttl
        self._request_timeout = request_timeout
        self._cache_text: str | None = None
        self._cache_until: datetime | None = None

    async def build_message(self) -> str | None:
        if self._cache_text and self._cache_until and datetime.now(UTC) < self._cache_until:
            return self._cache_text

        stale_cache = self._cache_text

        try:
            channel_html = await self.fetch_channel_html()
            glide_url = extract_latest_glide_url(channel_html)
            if not glide_url:
                return None

            glide_html = await self.fetch_glide_html(glide_url)
            listings = parse_glide_listings(glide_html)
            if not listings:
                return None

            entries = await self.resolve_untappd_matches(listings)
        except Exception:
            LOGGER.exception("Beer top refresh failed")
            return stale_cache

        if not entries:
            return None

        grouped = rank_category_entries(entries)
        if not grouped:
            return None

        text = format_beer_message(grouped)
        self._cache_text = text
        self._cache_until = datetime.now(UTC) + self._cache_ttl
        return text

    async def resolve_untappd_matches(self, listings: list[GlideListing]) -> list[BeerEntry]:
        entries: list[BeerEntry] = []

        for listing in listings:
            try:
                query = listing.name
                if listing.brewery:
                    query = f"{listing.name} {listing.brewery}"
                search_html = await self.fetch_untappd_search_html(query)
                match = select_best_untappd_match(listing, parse_untappd_search_results(search_html))
                if match is None:
                    continue

                page_html = await self.fetch_untappd_beer_page_html(match.url)
                details = parse_untappd_beer_page(page_html)
            except Exception:
                LOGGER.exception("Beer enrichment failed for %s", listing.name)
                continue

            entries.append(
                BeerEntry(
                    name=match.name,
                    brewery=match.brewery,
                    style=match.style,
                    rating=details.rating,
                    rating_count=details.rating_count,
                )
            )

        return entries

    async def fetch_channel_html(self) -> str:
        return await self._fetch_text(CHANNEL_URL)

    async def fetch_glide_html(self, glide_url: str) -> str:
        return await self._fetch_text(glide_url)

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
