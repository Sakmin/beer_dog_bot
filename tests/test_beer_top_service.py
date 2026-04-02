import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
import json

from beer_top import (
    BeerTopService,
    BeerEntry,
    BeerSearchQuery,
    GlideListing,
    UntappdSearchResult,
    extract_glide_app_id,
    extract_inventory_table_doc_id,
    merge_search_queries,
    parse_search_query,
    parse_firestore_inventory_rows,
    parse_glide_listings,
    parse_untappd_beer_page,
    parse_untappd_search_results,
    select_best_untappd_match,
)


FIXTURES = Path(__file__).parent / "fixtures"


def test_parse_glide_listings_reads_fixture_backed_payload():
    html = (FIXTURES / "glide_listing_sample.html").read_text()

    listings = parse_glide_listings(html)

    assert listings == [
        GlideListing(name="Berry Blast Smoothie", brewery="Funky Brewery"),
    ]


def test_parse_glide_listings_ignores_unrelated_json_metadata():
    html = """
    <html>
      <body>
        <script type="application/json">
          {"screen": {"title": "Inventory"}}
        </script>
      </body>
    </html>
    """

    assert parse_glide_listings(html) == []


def test_parse_glide_listings_ignores_name_only_unrelated_object():
    html = """
    <html>
      <body>
        <script type="application/json">
          {"name": "Inventory"}
        </script>
      </body>
    </html>
    """

    assert parse_glide_listings(html) == []


def test_extract_glide_app_id_reads_play_url():
    assert (
        extract_glide_app_id("https://go.glideapps.com/play/X56sGynCgXQC4bzpzWIm")
        == "X56sGynCgXQC4bzpzWIm"
    )


def test_extract_inventory_table_doc_id_uses_published_schema():
    document = """
    {
      "fields": {
        "schema": {
          "stringValue": "{\\"tables\\":[{\\"name\\":{\\"isSpecial\\":false,\\"name\\":\\"50c867b5-a28d-45b0-9f39-3775b2c42586\\"},\\"columns\\":[{\\"name\\":\\"ПИВОВАРНЯ\\"},{\\"name\\":\\"НАЗВАНИЕ\\"},{\\"name\\":\\"СТИЛЬ\\"},{\\"name\\":\\"ДОСТУПНО В БАРЕ\\"}]}]}"
        }
      }
    }
    """

    assert extract_inventory_table_doc_id(document) == "_50c867b5-a28d-45b0-9f39-3775b2c42586"


def test_parse_firestore_inventory_rows_filters_available_beers():
    rows = """
    {
      "documents": [
        {
          "fields": {
            "НАЗВАНИЕ": {"stringValue": "Green Jelly"},
            "ПИВОВАРНЯ": {"stringValue": "Hop Head"},
            "СТИЛЬ": {"stringValue": "Sour - Smoothie / Pastry"},
            "ДОСТУПНО В БАРЕ": {"booleanValue": true},
            "ОПИСАНИЕ": {"stringValue": "Lime, vanilla, marshmallow"},
            "ОТКРЫТЬ В UNTAPPD": {"stringValue": "https://untappd.com/b/hop-head/green-jelly/1"}
          }
        },
        {
          "fields": {
            "НАЗВАНИЕ": {"stringValue": "Hidden Draft"},
            "ПИВОВАРНЯ": {"stringValue": "Ghost Taproom"},
            "СТИЛЬ": {"stringValue": "IPA - American"},
            "ДОСТУПНО В БАРЕ": {"booleanValue": false},
            "ОТКРЫТЬ В UNTAPPD": {"stringValue": "https://untappd.com/b/ghost/hidden/2"}
          }
        }
      ]
    }
    """

    assert parse_firestore_inventory_rows(rows) == [
        GlideListing(
            name="Green Jelly",
            brewery="Hop Head",
            style="Sour - Smoothie / Pastry",
            untappd_url="https://untappd.com/b/hop-head/green-jelly/1",
            flavor_notes="Lime, vanilla, marshmallow",
        )
    ]


def test_parse_firestore_inventory_rows_falls_back_to_style_parentheses_for_flavor_notes():
    rows = """
    {
      "documents": [
        {
          "fields": {
            "НАЗВАНИЕ": {"stringValue": "Poetry of Love"},
            "ПИВОВАРНЯ": {"stringValue": "Rewort Brewery"},
            "СТИЛЬ": {"stringValue": "New England IPA (Chinook, Columbus, сок мандарина)"},
            "ДОСТУПНО В БАРЕ": {"booleanValue": true},
            "ОТКРЫТЬ В UNTAPPD": {"stringValue": "https://untappd.com/b/rewort/poetry-of-love/1"}
          }
        }
      ]
    }
    """

    assert parse_firestore_inventory_rows(rows) == [
        GlideListing(
            name="Poetry of Love",
            brewery="Rewort Brewery",
            style="New England IPA (Chinook, Columbus, сок мандарина)",
            untappd_url="https://untappd.com/b/rewort/poetry-of-love/1",
            flavor_notes="Chinook, Columbus, сок мандарина",
        )
    ]


def test_parse_untappd_search_results_extracts_server_rendered_result():
    html = (FIXTURES / "untappd_search_sample.html").read_text()

    results = parse_untappd_search_results(html)

    assert len(results) == 2
    assert results[0].name == "Berry Blast Smoothie"
    assert results[0].brewery == "Funky Brewery"
    assert results[0].style == "Sour - Smoothie / Pastry"
    assert results[0].url == "https://untappd.com/b/funky-brewery/berry-blast-smoothie/12345"


def test_parse_untappd_search_results_handles_nested_wrapper_markup():
    html = """
    <html>
      <body>
        <div class="beer-item">
          <div class="beer-details">
            <p class="name">
              <span class="label">Beer</span>
              <a href="/b/funky-brewery/berry-blast-smoothie/12345">
                <span>Berry</span> <strong>Blast Smoothie</strong>
              </a>
            </p>
            <div class="meta">
              <p class="brewery"><a href="/funky-brewery"><span>Funky Brewery</span></a></p>
              <p class="style"><span>Sour</span> - Smoothie / Pastry</p>
            </div>
          </div>
        </div>
      </body>
    </html>
    """

    results = parse_untappd_search_results(html)

    assert len(results) == 1
    assert results[0].name == "Berry Blast Smoothie"
    assert results[0].brewery == "Funky Brewery"
    assert results[0].style == "Sour - Smoothie / Pastry"
    assert results[0].url == "https://untappd.com/b/funky-brewery/berry-blast-smoothie/12345"


def test_parse_untappd_beer_page_extracts_rating_and_count():
    html = """
    <html>
      <head>
        <meta
          name="description"
          content="Berry Blast Smoothie by Funky Brewery is a Sour - Smoothie / Pastry which has a rating of 4.18 out of 5, with 1,248 ratings and reviews on Untappd."
        />
        <script type="application/ld+json">
          {
            "@context": "https://schema.org",
            "@type": "Product",
            "name": "Berry Blast Smoothie",
            "aggregateRating": {
              "@type": "AggregateRating",
              "ratingValue": "4.18",
              "reviewCount": "1248"
            }
          }
        </script>
      </head>
      <body>
        <div>4.3% ABV</div>
        <div>10 IBU</div>
      </body>
    </html>
    """

    details = parse_untappd_beer_page(html)

    assert details.rating == 4.18
    assert details.rating_count == 1248
    assert details.abv == 4.3
    assert details.ibu == 10


def test_parse_untappd_beer_page_falls_back_to_meta_when_json_ld_numbers_are_bad():
    html = """
    <html>
      <head>
        <meta
          name="description"
          content="Berry Blast Smoothie has a rating of 4.18 out of 5, with 1,248 ratings and reviews on Untappd."
        />
        <script type="application/ld+json">
          {
            "@type": "Product",
            "aggregateRating": {
              "ratingValue": "not-a-number",
              "reviewCount": "still-bad"
            }
          }
        </script>
      </head>
      <body>
        <div>6.5% ABV</div>
      </body>
    </html>
    """

    details = parse_untappd_beer_page(html)

    assert details.rating == 4.18
    assert details.rating_count == 1248
    assert details.abv == 6.5
    assert details.ibu is None


def test_select_best_untappd_match_rejects_same_name_wrong_brewery_candidate():
    listing = GlideListing(name="Berry Blast Smoothie", brewery="Funky Brewery")
    results = [
        UntappdSearchResult(
            name="Berry Blast Smoothie",
            brewery="Different Brewery",
            style="Sour - Smoothie / Pastry",
            url="https://untappd.com/b/different-brewery/berry-blast-smoothie/999",
        )
    ]

    assert select_best_untappd_match(listing, results) is None


def test_select_best_untappd_match_rejects_partial_overlap_wrong_brewery_candidate():
    listing = GlideListing(name="Green Flowers", brewery="Other Half Brewing")
    results = [
        UntappdSearchResult(
            name="Green Flowers",
            brewery="Other Side Brewing",
            style="IPA - Imperial / Double",
            url="https://untappd.com/b/other-side-brewing/green-flowers/321",
        )
    ]

    assert select_best_untappd_match(listing, results) is None


def test_build_message_returns_none_on_total_upstream_failure():
    service = BeerTopService()

    async def broken_fetch() -> str:
        raise RuntimeError("network down")

    service.fetch_channel_html = broken_fetch

    message = asyncio.run(service.build_message())

    assert message is None


def test_build_message_reads_from_cache_file(tmp_path):
    cache_path = tmp_path / "beer_inventory_cache.json"
    cache_path.write_text(
        json.dumps(
            {
                "refreshed_at": "2026-04-01T12:00:00+00:00",
                "source_glide_url": "https://go.glideapps.com/play/current",
                "inventory": [
                    {
                        "name": "Poetry of Love",
                        "brewery": "Rewort Brewery",
                        "style": "New England IPA",
                        "rating": 4.18,
                        "rating_count": 1800,
                        "alc": "6,9/30/16",
                        "flavor_notes": "Simcoe, mandarin",
                        "untappd_url": None,
                        "rating_available": True,
                        "untappd_abv": 4.3,
                        "untappd_ibu": 10,
                    }
                ],
                "ranked_entries": [
                    {
                        "name": "Poetry of Love",
                        "brewery": "Rewort Brewery",
                        "style": "New England IPA",
                        "rating": 4.18,
                        "rating_count": 1800,
                        "alc": "6,9/30/16",
                        "flavor_notes": "Simcoe, mandarin",
                        "untappd_url": None,
                        "rating_available": True,
                        "untappd_abv": 4.3,
                        "untappd_ibu": 10,
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    service = BeerTopService(cache_path=cache_path)

    message = asyncio.run(service.build_message())

    assert message is not None
    assert "Poetry of Love" in message
    assert "4.3% ABV/ 10 IBU" in message


def test_refresh_cache_writes_entries_to_disk(tmp_path):
    cache_path = tmp_path / "beer_inventory_cache.json"
    service = BeerTopService(cache_path=cache_path)

    entries = [
        BeerEntry(
            name="Poetry of Love",
            brewery="Rewort Brewery",
            style="New England IPA",
            rating=4.18,
            rating_count=1800,
            alc="6,9/30/16",
            flavor_notes="Simcoe, mandarin",
            rating_available=True,
        ),
        BeerEntry(
            name="Unknown Draft",
            brewery="Mystery Brew",
            style="IPA",
            rating=0.0,
            rating_count=0,
            alc="6,5/20/12",
            flavor_notes="resin, citrus",
            rating_available=False,
        ),
    ]

    async def fake_fetch_live_entries():
        return entries, "https://go.glideapps.com/play/current"

    service.fetch_live_entries = fake_fetch_live_entries

    count = asyncio.run(service.refresh_cache())

    assert count == 2
    payload = json.loads(cache_path.read_text(encoding="utf-8"))
    assert payload["source_glide_url"] == "https://go.glideapps.com/play/current"
    assert payload["inventory"][0]["name"] == "Poetry of Love"
    assert payload["inventory"][0]["untappd_abv"] is None
    assert payload["inventory"][0]["untappd_ibu"] is None
    assert len(payload["inventory"]) == 2
    assert len(payload["ranked_entries"]) == 1


def test_fetch_firestore_inventory_returns_full_listing_set_by_default():
    service = BeerTopService()

    async def fake_published_data_document(app_id: str) -> str:
        return """
        {
          "fields": {
            "schema": {
              "stringValue": "{\\"tables\\":[{\\"name\\":{\\"isSpecial\\":false,\\"name\\":\\"50c867b5-a28d-45b0-9f39-3775b2c42586\\"},\\"columns\\":[{\\"name\\":\\"ПИВОВАРНЯ\\"},{\\"name\\":\\"НАЗВАНИЕ\\"},{\\"name\\":\\"СТИЛЬ\\"},{\\"name\\":\\"ДОСТУПНО В БАРЕ\\"}]}]}"
            }
          }
        }
        """

    async def fake_table_rows_pages(app_id: str, table_doc_id: str) -> list[str]:
        return [
            """
            {
              "documents": [
                {
                  "fields": {
                    "НАЗВАНИЕ": {"stringValue": "Alpha"},
                    "ПИВОВАРНЯ": {"stringValue": "Brew One"},
                    "СТИЛЬ": {"stringValue": "IPA"},
                    "ДОСТУПНО В БАРЕ": {"booleanValue": true}
                  }
                },
                {
                  "fields": {
                    "НАЗВАНИЕ": {"stringValue": "Beta"},
                    "ПИВОВАРНЯ": {"stringValue": "Brew Two"},
                    "СТИЛЬ": {"stringValue": "Sour Ale"},
                    "ДОСТУПНО В БАРЕ": {"booleanValue": true}
                  }
                }
              ]
            }
            """
        ]

    service.fetch_published_data_document = fake_published_data_document
    service.fetch_table_rows_pages = fake_table_rows_pages

    listings = asyncio.run(service.fetch_firestore_inventory("app-id"))

    assert [listing.name for listing in listings] == ["Alpha", "Beta"]


def test_search_message_returns_exact_matches():
    service = BeerTopService()

    entries = [
        BeerEntry(
            name="Poetry of Love",
            brewery="Rewort Brewery",
            style="New England IPA",
            rating=4.18,
            rating_count=1800,
            alc="6,9/30/16",
            flavor_notes="Simcoe, mandarin",
        ),
        BeerEntry(
            name="Big Bitter",
            brewery="Hop Lab",
            style="IPA",
            rating=4.25,
            rating_count=2200,
            alc="7,8/50/18",
            flavor_notes="Simcoe, pine",
        ),
    ]

    service.load_cached_inventory = lambda: entries

    async def fake_rerank(query_text: str, candidates):
        assert query_text == "ne ipa simcoe до 7 градусов с высоким рейтингом"
        return [candidates[0]]

    service.rerank_candidates_with_llm = fake_rerank

    message = asyncio.run(service.search_message("ne ipa simcoe до 7 градусов с высоким рейтингом"))

    assert message is not None
    assert "Вот что нашел по запросу" in message
    assert "Poetry of Love" in message
    assert "6.9% ABV/ 30 IBU" in message
    assert "Big Bitter" not in message


def test_search_message_handles_russian_threshold_query_without_fallback():
    service = BeerTopService()

    entries = [
        BeerEntry(
            name="Periferiya",
            brewery="Big Village Brewery",
            style="New England IPA",
            rating=4.06,
            rating_count=3057,
            alc="7/-/17",
            flavor_notes="Citra, Galaxy",
        ),
        BeerEntry(
            name="Cowboy Marlboro",
            brewery="Plan B",
            style="IPA",
            rating=3.94,
            rating_count=9566,
            alc="6,5/50/16",
            flavor_notes="pine, citrus",
        ),
    ]

    service.load_cached_inventory = lambda: entries

    async def fake_rerank(query_text: str, candidates):
        return candidates

    service.rerank_candidates_with_llm = fake_rerank

    message = asyncio.run(
        service.search_message("найди ne ipa с рейтингом выше 3,99 и алкоголем не больше 7 градусов")
    )

    assert message is not None
    assert "Вот что нашел по запросу" in message
    assert "Periferiya" in message
    assert "Cowboy Marlboro" not in message


def test_search_message_falls_back_to_closest_matches():
    service = BeerTopService()

    entries = [
        BeerEntry(
            name="Poetry of Love",
            brewery="Rewort Brewery",
            style="New England IPA",
            rating=4.18,
            rating_count=1800,
            alc="7,3/35/17",
            flavor_notes="Chinook, Columbus, сок мандарина",
        ),
        BeerEntry(
            name="Soft Wheat",
            brewery="Brew Farm",
            style="Hefeweizen",
            rating=3.95,
            rating_count=400,
            alc="5,1/15/12",
            flavor_notes="banana, clove",
        ),
    ]

    service.load_cached_inventory = lambda: entries

    async def fake_rerank(query_text: str, candidates):
        return [candidates[-1], candidates[0]]

    service.rerank_candidates_with_llm = fake_rerank

    message = asyncio.run(service.search_message("ne ipa simcoe до 6 градусов"))

    assert message is not None
    assert "Точного совпадения по запросу" in message
    assert "Poetry of Love" in message
    assert "Soft Wheat" not in message


def test_search_message_fallback_skips_excluded_categories_when_possible():
    service = BeerTopService()

    entries = [
        BeerEntry(
            name="Boston Pie",
            brewery="Kulinar",
            style="Sour Ale",
            rating=4.31,
            rating_count=2471,
            alc="6/-/-",
            flavor_notes="cranberry, biscuit",
        ),
        BeerEntry(
            name="Cowboy Marlboro",
            brewery="Plan B",
            style="IPA",
            rating=3.94,
            rating_count=9566,
            alc="6,5/50/16",
            flavor_notes="pine, citrus",
        ),
        BeerEntry(
            name="Soft Wheat",
            brewery="Brew Farm",
            style="Hefeweizen",
            rating=3.95,
            rating_count=400,
            alc="5,1/15/12",
            flavor_notes="banana, clove",
        ),
    ]

    async def fake_parse_user_query(query_text: str):
        return BeerSearchQuery(
            raw_text=query_text,
            max_alc=6.5,
            min_rating=4.0,
            tokens=("juicy", "aromatic"),
            exclude_categories=("Sour Ale", "Pastry Sour Ale"),
        )

    service.load_cached_inventory = lambda: entries
    service.parse_user_query = fake_parse_user_query

    message = asyncio.run(service.search_message("не sour"))

    assert message is not None
    assert "Boston Pie" not in message
    assert "Soft Wheat" in message


def test_search_message_does_not_fallback_outside_requested_category():
    service = BeerTopService()

    entries = [
        BeerEntry(
            name="Splurge",
            brewery="Red Button",
            style="Pastry Sour Ale",
            rating=4.39,
            rating_count=3542,
            alc="7/-/-",
            flavor_notes="манго, маракуйя",
        ),
        BeerEntry(
            name="Unbreakfast",
            brewery="Rewort Brewery",
            style="Sour Ale",
            rating=4.26,
            rating_count=4582,
            alc="6/10/11,3",
            flavor_notes="черника, черная смородина",
        ),
    ]

    async def fake_parse_user_query(query_text: str):
        return BeerSearchQuery(
            raw_text=query_text,
            categories=("Weizen",),
            min_rating=4.0,
        )

    service.load_cached_inventory = lambda: entries
    service.parse_user_query = fake_parse_user_query

    message = asyncio.run(service.search_message("найди weizen с рейтингом выше 4"))

    assert message is not None
    assert "не нашел подходящих вариантов" in message
    assert "Weizen" in message
    assert "Splurge" not in message


def test_search_message_uses_full_inventory_not_only_ranked_entries():
    service = BeerTopService()

    inventory = [
        BeerEntry(
            name="Hidden Weizen",
            brewery="Brew Farm",
            style="Hefeweizen",
            rating=0.0,
            rating_count=0,
            alc="5,1/15/12",
            flavor_notes="banana, clove",
            rating_available=False,
        )
    ]

    async def fake_parse_user_query(query_text: str):
        return BeerSearchQuery(
            raw_text=query_text,
            categories=("Weizen",),
        )

    service.load_cached_inventory = lambda: inventory
    service.parse_user_query = fake_parse_user_query

    message = asyncio.run(service.search_message("найди weizen"))

    assert message is not None
    assert "Hidden Weizen" in message


def test_search_message_prefers_direct_flavor_matches_in_fallback():
    service = BeerTopService()

    entries = [
        BeerEntry(
            name="Lost Planet: Strawberry & Basil",
            brewery="Sabotage",
            style="Sour Ale",
            rating=4.12,
            rating_count=4418,
            alc="4,5/-/12",
            flavor_notes="клубника, базилик",
        ),
        BeerEntry(
            name="Dosa [Mango + Strawberry]",
            brewery="4BREWERS",
            style="Pastry Sour Ale",
            rating=4.22,
            rating_count=1574,
            alc="6/-/16",
            flavor_notes="манго, клубника",
        ),
        BeerEntry(
            name="Ne Krichi Na Kimchi",
            brewery="4BREWERS",
            style="Sour Ale",
            rating=4.25,
            rating_count=4210,
            alc="5/-/-",
            flavor_notes="соус кимчи",
        ),
    ]

    async def fake_parse_user_query(query_text: str):
        return BeerSearchQuery(
            raw_text=query_text,
            max_alc=6.7,
            tokens=("клубника",),
        )

    service.load_cached_inventory = lambda: entries
    service.parse_user_query = fake_parse_user_query

    message = asyncio.run(service.search_message("найди пиво со вкусом клубники меньше 6,7 градусов"))

    assert message is not None
    assert "Lost Planet: Strawberry &amp; Basil" in message
    assert "Dosa [Mango + Strawberry]" in message
    assert "Ne Krichi Na Kimchi" not in message


def test_merge_search_queries_prefers_llm_filters_and_keeps_rule_tokens():
    merged = merge_search_queries(
        BeerSearchQuery(
            raw_text="ne ipa simcoe",
            categories=("New England IPA",),
            tokens=("simcoe",),
        ),
        BeerSearchQuery(
            raw_text="ne ipa simcoe",
            categories=("IPA",),
            max_alc=7.0,
            min_rating=4.0,
            tokens=("citra",),
        ),
    )

    assert merged.categories == ("IPA", "New England IPA")
    assert merged.max_alc == 7.0
    assert merged.min_rating == 4.0
    assert merged.tokens == ("simcoe", "citra")


def test_parse_user_query_merges_llm_result_when_available():
    service = BeerTopService()

    async def fake_llm_parse(query_text: str):
        assert query_text == "что-то сочное, не sour, до 6.5"
        return BeerSearchQuery(
            raw_text=query_text,
            exclude_categories=("Sour Ale", "Pastry Sour Ale"),
            max_alc=6.5,
            tokens=("juicy", "citra"),
        )

    service.parse_query_with_llm = fake_llm_parse

    query = asyncio.run(service.parse_user_query("что-то сочное, не sour, до 6.5"))

    assert query.max_alc == 6.5
    assert query.exclude_categories == ("Sour Ale", "Pastry Sour Ale")
    assert "juicy" in query.tokens


def test_build_message_uses_memory_cache_for_repeated_requests():
    service = BeerTopService()
    calls = {"load": 0}
    entries = [
        BeerEntry(
            name="Poetry of Love",
            brewery="Rewort Brewery",
            style="New England IPA",
            rating=4.18,
            rating_count=1800,
            alc="6,9/30/16",
            flavor_notes="Simcoe, mandarin",
        )
    ]

    def fake_load_cached_entries():
        calls["load"] += 1
        return entries

    service.load_cached_entries = fake_load_cached_entries

    first = asyncio.run(service.build_message())
    second = asyncio.run(service.build_message())

    assert first == second
    assert calls == {"load": 1}
