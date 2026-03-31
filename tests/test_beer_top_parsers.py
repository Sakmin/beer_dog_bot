from pathlib import Path

from beer_top import BeerEntry, extract_latest_glide_url, format_beer_message


FIXTURES = Path(__file__).parent / "fixtures"


def test_extract_latest_glide_url_returns_most_recent_match():
    html = (FIXTURES / "telegram_channel_with_glide.html").read_text()

    assert extract_latest_glide_url(html) == "https://go.glideapps.com/play/current"


def test_extract_latest_glide_url_prefers_last_link_inside_one_post():
    html = """
    <html>
      <body>
        <div class="tgme_widget_message_wrap" data-post="beerhounds73/300">
          <div class="tgme_widget_message_text">
            <a href="https://go.glideapps.com/play/first">First link</a>
            <a href="https://go.glideapps.com/play/last">Last link</a>
          </div>
        </div>
      </body>
    </html>
    """

    assert extract_latest_glide_url(html) == "https://go.glideapps.com/play/last"


def test_extract_latest_glide_url_returns_none_when_missing():
    html = (FIXTURES / "telegram_channel_without_glide.html").read_text()

    assert extract_latest_glide_url(html) is None


def test_format_beer_message_groups_only_non_empty_categories():
    message = format_beer_message(
        {
            "New England IPA": [
                BeerEntry("Alpha", "Brewery", "IPA - New England / Hazy", 4.12, 1234)
            ],
            "IPA": [BeerEntry("Beta", "Brewery Two", "IPA - American", 4.01, 200)],
            "Sour Ale": [],
            "Pastry Sour Ale": [],
            "Безалкогольное": [
                BeerEntry("Gamma", None, "Безалкогольное", 3.88, 17)
            ],
        }
    )

    assert (
        message
        == "\n".join(
            [
                "Смотри какое интересное пиво я нашел:",
                "",
                "New England IPA",
                "Alpha - Brewery | Untappd 4.12 | 1,234 ratings",
                "",
                "IPA",
                "Beta - Brewery Two | Untappd 4.01 | 200 ratings",
                "",
                "Безалкогольное",
                "Gamma | Untappd 3.88 | 17 ratings",
            ]
        )
    )
