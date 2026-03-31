from __future__ import annotations

from dataclasses import dataclass
import math
import re
from html.parser import HTMLParser


CATEGORY_ORDER = (
    "New England IPA",
    "IPA",
    "Sour Ale",
    "Pastry Sour Ale",
    "Безалкогольное",
)


@dataclass(slots=True)
class BeerEntry:
    name: str
    brewery: str | None
    style: str
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


def extract_latest_glide_url(html: str) -> str | None:
    parser = _TelegramGlideURLParser()
    parser.feed(html)
    return parser.result


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
