from __future__ import annotations

from dataclasses import dataclass
import math


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
