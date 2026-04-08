import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
import json

from beer_top import (
    BeerTopService,
    BeerEntry,
    BeerSearchQuery,
    GlideListing,
    SergeyTopEntry,
    UntappdSearchResult,
    extract_glide_app_id,
    extract_inventory_table_doc_id,
    merge_search_queries,
    parse_search_query,
    parse_firestore_inventory_rows,
    parse_glide_listings,
    parse_untappd_beer_page,
    parse_untappd_user_beers_page,
    parse_untappd_user_top_entries_page,
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


def test_parse_untappd_user_beers_page_reads_only_distinct_beer_items():
    html = """
    <html>
      <body>
        <div class="distinct-list-list-container">
          <div class="beer-item" data-bid="6284259">
            <p class="name"><a href="/b/salden-s-brewery-east-coast-session-ipa/6284259">East Coast Session IPA</a></p>
          </div>
          <div class="beer-item" data-bid="6315798">
            <p class="name"><a href="/b/rewort-brewery-razverni-volnu/6315798">Разверни волну</a></p>
          </div>
        </div>
        <div class="sidebar">
          <a href="/b/plan-b-brewery-kovboj-malboro/673144">Sidebar beer should be ignored</a>
        </div>
      </body>
    </html>
    """

    urls = parse_untappd_user_beers_page(html)

    assert urls == [
        "https://untappd.com/b/salden-s-brewery-east-coast-session-ipa/6284259",
        "https://untappd.com/b/rewort-brewery-razverni-volnu/6315798",
    ]


def test_parse_untappd_user_top_entries_page_reads_personal_and_global_ratings():
    html = """
    <div class="distinct-list-list-container">
      <div class="beer-item" data-bid="673144">
        <div class="beer-details">
          <p class="name"><a href="/b/plan-b-brewery-kovboj-malboro/673144">Ковбой Мальборо</a></p>
          <p class="brewery"><a href="/Plan_B_Brewery">Plan B Brewery</a></p>
          <p class="style">IPA - American</p>
          <div class="ratings">
            <div class="you">
              <p>Their Rating (4)</p>
            </div>
            <div class="you">
              <p>Global Rating (3.94)</p>
            </div>
          </div>
        </div>
        <div class="details">
          <p class="abv">6.5% ABV</p>
          <p class="ibu">50 IBU</p>
          <p class="check-ins">Total: 1</p>
        </div>
      </div>
      <div class="beer-item" data-bid="6284259">
        <div class="beer-details">
          <p class="name"><a href="/b/salden-s-brewery-east-coast-session-ipa/6284259">East Coast Session IPA</a></p>
          <p class="brewery"><a href="/Saldens">Salden's Brewery</a></p>
          <p class="style">Session IPA (Galaxy)</p>
          <div class="ratings">
            <div class="you">
              <p>Their Rating (4.5)</p>
            </div>
            <div class="you">
              <p>Global Rating (3.80)</p>
            </div>
          </div>
        </div>
        <div class="details">
          <p class="abv">4.5% ABV</p>
        </div>
      </div>
    </div>
    """

    entries = parse_untappd_user_top_entries_page(html)

    assert entries == [
        SergeyTopEntry(
            name="Ковбой Мальборо",
            brewery="Plan B Brewery",
            style="IPA - American",
            personal_rating=4.0,
            global_rating=3.94,
            untappd_url="https://untappd.com/b/plan-b-brewery-kovboj-malboro/673144",
            untappd_abv=6.5,
            untappd_ibu=50,
        ),
        SergeyTopEntry(
            name="East Coast Session IPA",
            brewery="Salden's Brewery",
            style="Session IPA (Galaxy)",
            personal_rating=4.5,
            global_rating=3.8,
            untappd_url="https://untappd.com/b/salden-s-brewery-east-coast-session-ipa/6284259",
            untappd_abv=4.5,
            untappd_ibu=None,
        ),
    ]


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
    assert "🥃 4.3% | 🌲 10 IBU | ⭐ 4.18 | 👥 1,800" in message


def test_build_drink_already_message_excludes_exact_untappd_urls(tmp_path):
    cache_path = tmp_path / "beer_inventory_cache.json"
    cache_path.write_text(
        json.dumps(
            {
                "refreshed_at": "2026-04-01T12:00:00+00:00",
                "source_glide_url": "https://go.glideapps.com/play/current",
                "inventory": [],
                "ranked_entries": [
                    {
                        "name": "Alpha Session IPA",
                        "brewery": "Hop One",
                        "style": "Session IPA",
                        "rating": 4.11,
                        "rating_count": 1500,
                        "alc": "4,9/30/12",
                        "flavor_notes": "Galaxy",
                        "untappd_url": "https://untappd.com/b/hop-one/alpha-session-ipa/1",
                        "rating_available": True,
                        "untappd_abv": 4.9,
                        "untappd_ibu": 30,
                    },
                    {
                        "name": "Bravo Session IPA",
                        "brewery": "Hop Two",
                        "style": "Session IPA",
                        "rating": 4.02,
                        "rating_count": 900,
                        "alc": "4,8/28/12",
                        "flavor_notes": "Mosaic",
                        "untappd_url": "https://untappd.com/b/hop-two/bravo-session-ipa/2",
                        "rating_available": True,
                        "untappd_abv": 4.8,
                        "untappd_ibu": 28,
                    },
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    service = BeerTopService(cache_path=cache_path)

    async def fake_fetch_drunk_beer_urls(username: str) -> set[str]:
        assert username == "sergey_ulsk"
        return {"https://untappd.com/b/hop-one/alpha-session-ipa/1"}

    service.fetch_drunk_beer_urls = fake_fetch_drunk_beer_urls

    message = asyncio.run(service.build_drink_already_message())

    assert message is not None
    assert "Bravo Session IPA" in message
    assert "Alpha Session IPA" not in message


def test_build_drink_already_message_returns_none_without_cache(tmp_path):
    service = BeerTopService(cache_path=tmp_path / "missing.json")

    async def fake_fetch_drunk_beer_urls(username: str) -> set[str]:
        return set()

    service.fetch_drunk_beer_urls = fake_fetch_drunk_beer_urls

    message = asyncio.run(service.build_drink_already_message())

    assert message is None


def test_build_sergey_top_message_sorts_by_personal_then_global_rating():
    service = BeerTopService()

    async def fake_fetch_sergey_top_entries(username: str):
        assert username == "sergey_ulsk"
        return [
            SergeyTopEntry(
                name="Beer B",
                brewery="Brew B",
                style="IPA",
                personal_rating=4.5,
                global_rating=3.9,
                untappd_url="https://untappd.com/b/b/2",
                untappd_abv=6.0,
                untappd_ibu=40,
            ),
            SergeyTopEntry(
                name="Beer A",
                brewery="Brew A",
                style="Sour Ale",
                personal_rating=5.0,
                global_rating=3.5,
                untappd_url="https://untappd.com/b/a/1",
                untappd_abv=5.0,
                untappd_ibu=None,
            ),
            SergeyTopEntry(
                name="Beer C",
                brewery="Brew C",
                style="IPA",
                personal_rating=4.5,
                global_rating=4.1,
                untappd_url="https://untappd.com/b/c/3",
                untappd_abv=None,
                untappd_ibu=None,
            ),
        ]

    service.fetch_user_top_entries = fake_fetch_sergey_top_entries

    message = asyncio.run(service.build_sergey_top_message())

    assert message is not None
    assert "Beer A" in message
    assert message.index("Beer A") < message.index("Beer C") < message.index("Beer B")
    assert "🙋 5.00 | ⭐ 3.50 | 👥 -" in message
    assert "🙋 4.50 | ⭐ 4.10 | 👥 -" in message


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
        return entries, "https://go.glideapps.com/play/current", "2026-04-02"

    service.fetch_live_entries = fake_fetch_live_entries

    count = asyncio.run(service.refresh_cache())

    assert count == 2
    payload = json.loads(cache_path.read_text(encoding="utf-8"))
    assert payload["source_glide_url"] == "https://go.glideapps.com/play/current"
    assert payload["source_post_date"] == "2026-04-02"
    assert payload["inventory"][0]["name"] == "Poetry of Love"
    assert payload["inventory"][0]["untappd_abv"] is None
    assert payload["inventory"][0]["untappd_ibu"] is None
    assert len(payload["inventory"]) == 2
    assert len(payload["ranked_entries"]) == 1


def test_refresh_cache_preserves_previous_untappd_metrics_for_still_available_beer(tmp_path):
    cache_path = tmp_path / "beer_inventory_cache.json"
    cache_path.write_text(
        json.dumps(
            {
                "refreshed_at": "2026-04-01T12:00:00+00:00",
                "source_glide_url": "https://go.glideapps.com/play/old",
                "source_post_date": "2026-04-01",
                "inventory": [
                    {
                        "name": "East Coast Session IPA",
                        "brewery": "Salden's",
                        "style": "Session IPA (Galaxy)",
                        "rating": 3.80,
                        "rating_count": 521,
                        "alc": "4,5/-/11,5",
                        "flavor_notes": "Galaxy",
                        "untappd_url": "https://untappd.com/b/salden-s-brewery-east-coast-session-ipa/6284259",
                        "rating_available": True,
                        "untappd_abv": 4.5,
                        "untappd_ibu": 35,
                    },
                    {
                        "name": "Removed Beer",
                        "brewery": "Gone Brewery",
                        "style": "IPA",
                        "rating": 4.11,
                        "rating_count": 900,
                        "alc": "6,0/30/12",
                        "flavor_notes": None,
                        "untappd_url": "https://untappd.com/b/gone/removed/1",
                        "rating_available": True,
                        "untappd_abv": 6.0,
                        "untappd_ibu": 30,
                    },
                ],
                "ranked_entries": [],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    service = BeerTopService(cache_path=cache_path)

    entries = [
        BeerEntry(
            name="East Coast Session IPA",
            brewery="Salden's",
            style="Session IPA (Galaxy)",
            rating=3.8,
            rating_count=0,
            alc="4,5/-/11,5",
            flavor_notes="Galaxy",
            untappd_url="https://untappd.com/b/salden-s-brewery-east-coast-session-ipa/6284259",
            rating_available=False,
            untappd_abv=None,
            untappd_ibu=None,
        )
    ]

    async def fake_fetch_live_entries():
        return entries, "https://go.glideapps.com/play/current", "2026-04-02"

    service.fetch_live_entries = fake_fetch_live_entries

    count = asyncio.run(service.refresh_cache())

    assert count == 1
    payload = json.loads(cache_path.read_text(encoding="utf-8"))
    assert len(payload["inventory"]) == 1
    saved = payload["inventory"][0]
    assert saved["name"] == "East Coast Session IPA"
    assert saved["rating_available"] is True
    assert saved["rating"] == 3.8
    assert saved["rating_count"] == 521
    assert saved["untappd_abv"] == 4.5
    assert saved["untappd_ibu"] == 35
    assert all(item["name"] != "Removed Beer" for item in payload["inventory"])


def test_build_menu_export_reads_cached_inventory_and_uses_post_date(tmp_path):
    cache_path = tmp_path / "beer_inventory_cache.json"
    cache_path.write_text(
        json.dumps(
            {
                "refreshed_at": "2026-04-02T12:00:00+00:00",
                "source_glide_url": "https://go.glideapps.com/play/current",
                "source_post_date": "2026-04-01",
                "inventory": [
                    {
                        "name": "Ковбой Мальборо",
                        "brewery": "Plan B",
                        "style": "IPA",
                        "rating": 3.94,
                        "rating_count": 9567,
                        "alc": "6,5/50/16",
                        "flavor_notes": None,
                        "untappd_url": "https://untappd.com/b/plan-b/cowboy/1",
                        "rating_available": True,
                        "untappd_abv": 6.5,
                        "untappd_ibu": 50,
                    }
                ],
                "ranked_entries": [],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    service = BeerTopService(cache_path=cache_path)

    export = service.build_menu_export()

    assert export is not None
    filename, content = export
    assert filename == "beer_menu_2026-04-01.txt"
    assert "Меню Beer Hounds от 2026-04-01" in content
    assert "• Ковбой Мальборо - Plan B" in content
    assert "🥃 6.5% | 🌲 50 IBU | ⭐ 3.94 | 👥 9,567" in content
    assert "Untappd: https://untappd.com/b/plan-b/cowboy/1" in content


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
    assert "🥃 6.9% | 🌲 30 IBU | ⭐ 4.18 | 👥 1,800" in message
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


def test_search_message_finds_light_ipa_without_falling_back_to_stronger_options():
    service = BeerTopService()

    entries = [
        BeerEntry(
            name="Easy Session",
            brewery="Small Brewery",
            style="Session IPA",
            rating=3.82,
            rating_count=420,
            alc="4,7/25/12",
            flavor_notes="citrus, light body",
        ),
        BeerEntry(
            name="Cowboy Marlboro",
            brewery="Plan B",
            style="IPA",
            rating=3.94,
            rating_count=9567,
            alc="6,5/50/16",
            flavor_notes="pine, citrus",
        ),
    ]

    service.load_cached_inventory = lambda: entries

    async def fake_rerank(query_text: str, candidates):
        return candidates

    service.rerank_candidates_with_llm = fake_rerank

    message = asyncio.run(service.search_message("подскажи легкую IPA до 5,1 градуса алкоголя"))

    assert message is not None
    assert "Вот что нашел по запросу" in message
    assert "Easy Session" in message
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
