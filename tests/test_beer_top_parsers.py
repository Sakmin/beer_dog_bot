from pathlib import Path

from beer_top import (
    BeerEntry,
    extract_latest_glide_metadata,
    extract_latest_glide_url,
    format_beer_message,
)


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


def test_extract_latest_glide_metadata_returns_url_and_post_date():
    html = """
    <html>
      <body>
        <div class="tgme_widget_message_wrap js-message_group" data-post="beerhounds73/301">
          <div class="tgme_widget_message" data-post="beerhounds73/301">
            <a class="tgme_widget_message_date" href="https://t.me/beerhounds73/301">
              <time datetime="2026-04-02T11:30:00+00:00">Apr 2</time>
            </a>
            <div class="tgme_widget_message_text">
              <a href="https://go.glideapps.com/play/current-menu">Current availability</a>
            </div>
          </div>
        </div>
      </body>
    </html>
    """

    metadata = extract_latest_glide_metadata(html)

    assert metadata is not None
    assert metadata.glide_url == "https://go.glideapps.com/play/current-menu"
    assert metadata.post_date == "2026-04-02"


def test_format_beer_message_groups_only_non_empty_categories():
    message = format_beer_message(
        {
            "IPA для старта": [
                BeerEntry("Starter", "Easy Brew", "Session IPA", 3.91, 321, "4,7/25/12")
            ],
            "New England IPA": [
                BeerEntry(
                    "Alpha",
                    "Brewery (г. Москва)",
                    "IPA - New England / Hazy",
                    4.12,
                    1234,
                    "5/3/12,5",
                    "Chinook, Citra, mango",
                    untappd_abv=4.3,
                    untappd_ibu=10,
                )
            ],
            "IPA": [BeerEntry("Beta", "Brewery Two", "IPA - American", 4.01, 200, "6/40/16")],
            "Pastry Sour Ale": [],
            "Sour Ale": [],
            "Weizen": [BeerEntry("Delta", "Wheat House", "Hefeweizen", 3.97, 89, "5,4/12/13")],
            "Безалкогольное": [
                BeerEntry("Gamma", None, "Безалкогольное", 3.88, 17, None)
            ],
        }
    )

    assert (
        message
        == "\n".join(
            [
                "Смотри какое интересное пиво я нашел:",
                "",
                "<b>😌😌😌 IPA для старта 😌😌😌</b>",
                "• Starter - Easy Brew",
                "🥃 4.7% | 🌲 25 IBU | ⭐ 3.91 | 👥 321",
                "",
                "<b>🇺🇸🇺🇸🇺🇸 New England IPA 🇺🇸🇺🇸🇺🇸</b>",
                "• Alpha (Chinook, Citra, mango) - Brewery",
                "🥃 4.3% | 🌲 10 IBU | ⭐ 4.12 | 👥 1,234",
                "",
                "<b>🌲🌲🌲 IPA 🌲🌲🌲</b>",
                "• Beta - Brewery Two",
                "🥃 6.0% | 🌲 40 IBU | ⭐ 4.01 | 👥 200",
                "",
                "<b>🇩🇪🇩🇪🇩🇪 Weizen 🇩🇪🇩🇪🇩🇪</b>",
                "• Delta - Wheat House",
                "🥃 5.4% | 🌲 12 IBU | ⭐ 3.97 | 👥 89",
                "",
                "<b>🚫🚫🚫 Безалкогольное 🚫🚫🚫</b>",
                "• Gamma",
                "🥃 - | 🌲 - | ⭐ 3.88 | 👥 17",
            ]
        )
    )
