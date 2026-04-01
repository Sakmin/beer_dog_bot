from beer_top import BeerEntry, categorize_style, parse_search_query, rank_category_entries, weighted_score


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
    assert categorize_style("IPA - American", "0,5/20/10") == "Безалкогольное"
    assert categorize_style("IPA - Non-Alco") == "Безалкогольное"


def test_categorize_style_detects_weizen_family():
    assert categorize_style("Hefeweizen") == "Weizen"
    assert categorize_style("Wheat Beer") == "Weizen"


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


def test_parse_search_query_extracts_filters_and_tokens():
    query = parse_search_query("ne ipa simcoe до 7 градусов алкоголя с высоким рейтингом")

    assert query.categories == ("New England IPA", "IPA")
    assert query.max_alc == 7.0
    assert query.min_rating == 4.0
    assert "simcoe" in query.tokens


def test_parse_search_query_extracts_negative_category_preferences():
    query = parse_search_query("хочу что-то сочное, не sour, не ipa")

    assert set(query.exclude_categories) == {
        "Sour Ale",
        "Pastry Sour Ale",
        "IPA",
        "New England IPA",
    }


def test_parse_search_query_understands_russian_rating_and_alc_bounds():
    query = parse_search_query("найди ne ipa с рейтингом выше 3,99 и алкоголем не больше 7 градусов")

    assert "New England IPA" in query.categories
    assert query.max_alc == 7.0
    assert query.min_rating == 3.99
    assert query.tokens == ()
