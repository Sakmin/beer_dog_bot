from beer_top import BeerEntry, categorize_style, rank_category_entries, weighted_score


def test_categorize_style_separates_neipa_from_regular_ipa():
    assert categorize_style("IPA - New England / Hazy") == "New England IPA"
    assert categorize_style("IPA - American") == "IPA"


def test_categorize_style_detects_pastry_sour_before_generic_sour():
    assert categorize_style("Sour - Smoothie / Pastry") == "Pastry Sour Ale"
    assert categorize_style("Sour - Fruited") == "Sour Ale"


def test_categorize_style_does_not_make_non_sour_styles_pastry_sour():
    assert categorize_style("Milkshake IPA") == "IPA"
    assert categorize_style("Pastry Stout") is None


def test_categorize_style_handles_non_alcoholic_beer():
    assert categorize_style("Безалкогольное пиво") == "Безалкогольное"


def test_weighted_score_prefers_real_volume_over_tiny_samples():
    tiny_sample = BeerEntry(
        name="Tiny Sample",
        brewery="A",
        style="IPA",
        rating=4.8,
        rating_count=3,
    )
    crowd_favorite = BeerEntry(
        name="Crowd Favorite",
        brewery="B",
        style="IPA",
        rating=4.3,
        rating_count=2500,
    )

    assert weighted_score(crowd_favorite) > weighted_score(tiny_sample)


def test_rank_category_entries_prefers_rating_with_real_review_volume():
    beers = [
        BeerEntry(name="Tiny Sample", brewery="A", style="IPA", rating=4.8, rating_count=3),
        BeerEntry(
            name="Crowd Favorite",
            brewery="B",
            style="IPA",
            rating=4.3,
            rating_count=2500,
        ),
    ]

    ranked = rank_category_entries(beers)["IPA"]

    assert ranked[0].name == "Crowd Favorite"
