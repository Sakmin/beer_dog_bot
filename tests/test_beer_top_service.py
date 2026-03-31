import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

from beer_top import (
    BeerTopService,
    GlideListing,
    UntappdSearchResult,
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
    assert "Pastry Sour Ale" in message
    assert "Berry Blast Smoothie - Funky Brewery | Untappd 4.18 | 1,248 ratings" in message


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
