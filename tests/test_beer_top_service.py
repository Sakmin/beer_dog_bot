import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

from beer_top import (
    BeerTopService,
    GlideListing,
    UntappdSearchResult,
    extract_glide_app_id,
    extract_inventory_table_doc_id,
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
    html = (FIXTURES / "untappd_beer_page_sample.html").read_text()

    details = parse_untappd_beer_page(html)

    assert details.rating == 4.18
    assert details.rating_count == 1248


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
    </html>
    """

    details = parse_untappd_beer_page(html)

    assert details.rating == 4.18
    assert details.rating_count == 1248


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


def test_build_message_returns_text_on_success():
    service = BeerTopService()

    async def fake_channel_html() -> str:
        return """
        <div class="tgme_widget_message_wrap">
          <a href="https://go.glideapps.com/play/current">Current list</a>
        </div>
        """

    async def fake_glide_html(url: str) -> str:
        assert url == "https://go.glideapps.com/play/current"
        return (FIXTURES / "glide_listing_sample.html").read_text()

    async def fake_untappd_search_html(query: str) -> str:
        assert "Berry Blast Smoothie" in query
        return (FIXTURES / "untappd_search_sample.html").read_text()

    async def fake_untappd_beer_page_html(url: str) -> str:
        assert url == "https://untappd.com/b/funky-brewery/berry-blast-smoothie/12345"
        return (FIXTURES / "untappd_beer_page_sample.html").read_text()

    service.fetch_channel_html = fake_channel_html
    service.fetch_glide_html = fake_glide_html
    service.fetch_untappd_search_html = fake_untappd_search_html
    service.fetch_untappd_beer_page_html = fake_untappd_beer_page_html

    message = asyncio.run(service.build_message())

    assert message is not None
    assert "Смотри какое интересное пиво я нашел:" in message
    assert "<b><u>Pastry Sour Ale</u></b>" in message
    assert "• <b>Berry Blast Smoothie</b> - Funky Brewery" in message
    assert "Untappd 4.18 | 1,248 ratings" in message


def test_build_message_falls_back_to_firestore_inventory_when_glide_html_is_shell_only():
    service = BeerTopService()

    async def fake_channel_html() -> str:
        return """
        <div class="tgme_widget_message_wrap">
          <a href="https://go.glideapps.com/play/X56sGynCgXQC4bzpzWIm">Current list</a>
        </div>
        """

    async def fake_glide_html(url: str) -> str:
        assert url == "https://go.glideapps.com/play/X56sGynCgXQC4bzpzWIm"
        return "<html><body><div id='root'></div></body></html>"

    async def fake_published_data_document(app_id: str) -> str:
        assert app_id == "X56sGynCgXQC4bzpzWIm"
        return """
        {
          "fields": {
            "schema": {
              "stringValue": "{\\"tables\\":[{\\"name\\":{\\"isSpecial\\":false,\\"name\\":\\"50c867b5-a28d-45b0-9f39-3775b2c42586\\"},\\"columns\\":[{\\"name\\":\\"ПИВОВАРНЯ\\"},{\\"name\\":\\"НАЗВАНИЕ\\"},{\\"name\\":\\"СТИЛЬ\\"},{\\"name\\":\\"ДОСТУПНО В БАРЕ\\"}]}]}"
            }
          }
        }
        """

    async def fake_table_rows(app_id: str, table_doc_id: str) -> list[str]:
        assert app_id == "X56sGynCgXQC4bzpzWIm"
        assert table_doc_id == "_50c867b5-a28d-45b0-9f39-3775b2c42586"
        return [
            """
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
                }
              ]
            }
            """
        ]

    async def fake_untappd_beer_page_html(url: str) -> str:
        assert url == "https://untappd.com/b/hop-head/green-jelly/1"
        return (FIXTURES / "untappd_beer_page_sample.html").read_text()

    service.fetch_channel_html = fake_channel_html
    service.fetch_glide_html = fake_glide_html
    service.fetch_published_data_document = fake_published_data_document
    service.fetch_table_rows_pages = fake_table_rows
    service.fetch_untappd_beer_page_html = fake_untappd_beer_page_html

    message = asyncio.run(service.build_message())

    assert message is not None
    assert "<b><u>Pastry Sour Ale</u></b>" in message
    assert "• <b>Green Jelly</b> (Lime, vanilla, marshmallow) - Hop Head" in message
    assert "Untappd 4.18 | 1,248 ratings" in message


def test_build_message_returns_none_on_total_upstream_failure():
    service = BeerTopService()

    async def broken_fetch() -> str:
        raise RuntimeError("network down")

    service.fetch_channel_html = broken_fetch

    message = asyncio.run(service.build_message())

    assert message is None


def test_build_message_uses_cached_result_for_repeated_requests():
    service = BeerTopService()
    calls = {
        "channel": 0,
        "glide": 0,
        "search": 0,
        "page": 0,
    }

    async def fake_channel_html() -> str:
        calls["channel"] += 1
        return """
        <div class="tgme_widget_message_wrap">
          <a href="https://go.glideapps.com/play/current">Current list</a>
        </div>
        """

    async def fake_glide_html(url: str) -> str:
        calls["glide"] += 1
        return (FIXTURES / "glide_listing_sample.html").read_text()

    async def fake_untappd_search_html(query: str) -> str:
        calls["search"] += 1
        return (FIXTURES / "untappd_search_sample.html").read_text()

    async def fake_untappd_beer_page_html(url: str) -> str:
        calls["page"] += 1
        return (FIXTURES / "untappd_beer_page_sample.html").read_text()

    service.fetch_channel_html = fake_channel_html
    service.fetch_glide_html = fake_glide_html
    service.fetch_untappd_search_html = fake_untappd_search_html
    service.fetch_untappd_beer_page_html = fake_untappd_beer_page_html

    first = asyncio.run(service.build_message())
    second = asyncio.run(service.build_message())

    assert first == second
    assert calls == {
        "channel": 1,
        "glide": 1,
        "search": 1,
        "page": 1,
    }


def test_build_message_returns_stale_cache_when_refresh_fails_after_expiry():
    service = BeerTopService()
    service._cache_text = "cached beer message"
    service._cache_until = datetime.now(UTC) - timedelta(seconds=1)

    async def broken_fetch() -> str:
        raise RuntimeError("network down")

    service.fetch_channel_html = broken_fetch

    message = asyncio.run(service.build_message())

    assert message == "cached beer message"


def test_build_message_returns_none_for_successful_empty_refresh_after_expiry():
    service = BeerTopService()
    service._cache_text = "cached beer message"
    service._cache_until = datetime.now(UTC) - timedelta(seconds=1)

    async def fake_channel_html() -> str:
        return """
        <div class="tgme_widget_message_wrap">
          <div class="tgme_widget_message_text">No current Glide link here</div>
        </div>
        """

    service.fetch_channel_html = fake_channel_html

    message = asyncio.run(service.build_message())

    assert message is None
