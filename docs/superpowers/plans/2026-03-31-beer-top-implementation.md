# Beer Top Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a beer recommendation flow that sends two existing Wednesday polls first, then posts a ranked beer подборка from BeerHounds availability data, and exposes the same recommendation block through `/top_beer`.

**Architecture:** Keep Telegram scheduling and handlers in `bot.py`, and move beer-related parsing, enrichment, ranking, formatting, caching, and fallback orchestration into a dedicated `beer_top.py` module. Build the feature test-first with HTML fixtures and deterministic unit tests so third-party parser changes can be isolated and debugged quickly.

**Tech Stack:** Python, aiogram, httpx, BeautifulSoup4, pytest, pytest-asyncio

---

## File Structure

### Existing files to modify

- `bot.py`
  - Keep bot startup, poll sending, schedulers, and command handlers.
  - Integrate the new beer recommendation flow after the polls.
  - Add the `/top_beer` command handler and command registration.
- `requirements.txt`
  - Add runtime/test dependencies needed for async HTTP, HTML parsing, and tests.

### New files to create

- `beer_top.py`
  - Dedicated module for beer-related logic:
  - Telegram channel HTML parsing
  - Glide URL extraction
  - Glide availability parsing
  - Untappd search/page parsing
  - style categorization
  - weighted ranking
  - message formatting
  - in-memory cache
  - orchestration and fallback behavior
- `tests/test_beer_top_logic.py`
  - Unit tests for category mapping, weighted scoring, category ranking, and final top-5 trimming.
- `tests/test_beer_top_parsers.py`
  - Unit tests for Telegram HTML parsing, Glide extraction parsing, and Untappd HTML parsing with fixtures.
- `tests/test_beer_top_service.py`
  - Orchestration tests for success, partial match, cache hit, and safe failure behavior.
- `tests/test_bot_integration.py`
  - Tests for survey order and `/top_beer` command behavior using monkeypatched bot calls.
- `tests/fixtures/telegram_channel_with_glide.html`
  - Public Telegram channel sample containing at least one `go.glideapps.com/play/...` link.
- `tests/fixtures/telegram_channel_without_glide.html`
  - Negative fixture with no Glide link.
- `tests/fixtures/glide_listing_sample.html`
  - Captured Glide response sample or reduced HTML snippet used by the implemented parser.
- `tests/fixtures/untappd_search_sample.html`
  - Reduced Untappd search-result HTML fixture.
- `tests/fixtures/untappd_beer_page_sample.html`
  - Reduced Untappd beer-page fixture containing rating count/style data.

## Task 1: Set up dependencies and beer ranking primitives

**Files:**
- Modify: `requirements.txt`
- Create: `beer_top.py`
- Create: `tests/test_beer_top_logic.py`

- [ ] **Step 1: Write the failing logic tests**

```python
from beer_top import BeerEntry, categorize_style, rank_category_entries


def test_categorize_style_separates_neipa_from_regular_ipa():
    assert categorize_style("IPA - New England / Hazy") == "New England IPA"
    assert categorize_style("IPA - American") == "IPA"


def test_categorize_style_detects_pastry_sour_before_generic_sour():
    assert categorize_style("Sour - Smoothie / Pastry") == "Pastry Sour Ale"
    assert categorize_style("Sour - Fruited") == "Sour Ale"


def test_rank_category_entries_prefers_rating_with_real_review_volume():
    beers = [
        BeerEntry(name="Tiny Sample", brewery="A", style="IPA", rating=4.8, rating_count=3),
        BeerEntry(name="Crowd Favorite", brewery="B", style="IPA", rating=4.3, rating_count=2500),
    ]

    ranked = rank_category_entries(beers)["IPA"]

    assert ranked[0].name == "Crowd Favorite"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_beer_top_logic.py -q`

Expected: FAIL with `ModuleNotFoundError: No module named 'beer_top'`

- [ ] **Step 3: Write the minimal implementation**

```python
from dataclasses import dataclass
import math


@dataclass(slots=True)
class BeerEntry:
    name: str
    brewery: str | None
    style: str
    rating: float
    rating_count: int


def categorize_style(style: str) -> str | None:
    normalized = style.lower()
    if "non-alcoholic" in normalized:
        return "Безалкогольное"
    if "smoothie" in normalized or "pastry" in normalized:
        return "Pastry Sour Ale"
    if "new england" in normalized or "hazy" in normalized:
        return "New England IPA"
    if "sour" in normalized or "wild ale" in normalized:
        return "Sour Ale"
    if "ipa" in normalized:
        return "IPA"
    return None


def weighted_score(entry: BeerEntry) -> float:
    return entry.rating * math.log10(entry.rating_count + 10)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_beer_top_logic.py -q`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add requirements.txt beer_top.py tests/test_beer_top_logic.py
git commit -m "test: add beer ranking primitives"
```

## Task 2: Add Telegram and message-formatting parsers

**Files:**
- Modify: `beer_top.py`
- Create: `tests/test_beer_top_parsers.py`
- Create: `tests/fixtures/telegram_channel_with_glide.html`
- Create: `tests/fixtures/telegram_channel_without_glide.html`

- [ ] **Step 1: Write the failing parser/formatter tests**

```python
from pathlib import Path

from beer_top import extract_latest_glide_url, format_beer_message, BeerEntry


def test_extract_latest_glide_url_returns_most_recent_match():
    html = Path("tests/fixtures/telegram_channel_with_glide.html").read_text()
    assert extract_latest_glide_url(html).startswith("https://go.glideapps.com/play/")


def test_extract_latest_glide_url_returns_none_when_missing():
    html = Path("tests/fixtures/telegram_channel_without_glide.html").read_text()
    assert extract_latest_glide_url(html) is None


def test_format_beer_message_groups_only_non_empty_categories():
    message = format_beer_message({
        "IPA": [BeerEntry("Alpha", "Brewery", "IPA - American", 4.12, 1234)],
        "Sour Ale": [],
    })

    assert "Смотри какое интересное пиво я нашел:" in message
    assert "IPA" in message
    assert "Sour Ale" not in message
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_beer_top_parsers.py -q`

Expected: FAIL because `extract_latest_glide_url` and `format_beer_message` do not exist yet.

- [ ] **Step 3: Write the minimal implementation**

```python
from bs4 import BeautifulSoup


def extract_latest_glide_url(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    for message in soup.select(".tgme_widget_message_wrap"):
        link = message.select_one('a[href*="go.glideapps.com/play/"]')
        if link and link.get("href"):
            return link["href"]
    return None


def format_beer_message(grouped: dict[str, list[BeerEntry]]) -> str:
    lines = ["Смотри какое интересное пиво я нашел:"]
    for category in CATEGORY_ORDER:
        beers = grouped.get(category, [])
        if not beers:
            continue
        lines.append("")
        lines.append(category)
        for beer in beers[:5]:
            brewery = beer.brewery or "Unknown Brewery"
            lines.append(
                f"{beer.name} - {brewery} | Untappd {beer.rating:.2f} | {beer.rating_count:,} ratings"
            )
    return "\n".join(lines)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_beer_top_parsers.py -q`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add beer_top.py tests/test_beer_top_parsers.py tests/fixtures/telegram_channel_with_glide.html tests/fixtures/telegram_channel_without_glide.html
git commit -m "test: add telegram parsing and beer message formatting"
```

## Task 3: Add Glide/Untappd enrichment and orchestration with fallback

**Files:**
- Modify: `beer_top.py`
- Create: `tests/test_beer_top_service.py`
- Create: `tests/fixtures/glide_listing_sample.html`
- Create: `tests/fixtures/untappd_search_sample.html`
- Create: `tests/fixtures/untappd_beer_page_sample.html`

- [ ] **Step 1: Write the failing orchestration tests**

```python
import pytest

from beer_top import BeerTopService


@pytest.mark.asyncio
async def test_build_message_returns_text_on_success():
    service = BeerTopService()
    service.fetch_channel_html = fake_channel_html
    service.fetch_glide_html = fake_glide_html
    service.resolve_untappd_matches = fake_resolve_matches

    message = await service.build_message()

    assert message is not None
    assert "Смотри какое интересное пиво я нашел:" in message


@pytest.mark.asyncio
async def test_build_message_returns_none_on_total_upstream_failure():
    service = BeerTopService()

    async def broken_fetch(*args, **kwargs):
        raise RuntimeError("network down")

    service.fetch_channel_html = broken_fetch

    message = await service.build_message()

    assert message is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_beer_top_service.py -q`

Expected: FAIL because `BeerTopService` is not implemented yet.

- [ ] **Step 3: Write the minimal implementation**

```python
import asyncio
import httpx
import logging
from datetime import datetime, timedelta
from typing import Any


class BeerTopService:
    def __init__(self) -> None:
        self._cache_text: str | None = None
        self._cache_until: datetime | None = None

    async def build_message(self) -> str | None:
        if self._cache_until and datetime.utcnow() < self._cache_until and self._cache_text:
            return self._cache_text

        try:
            channel_html = await self.fetch_channel_html()
            glide_url = extract_latest_glide_url(channel_html)
            if not glide_url:
                return None
            glide_html = await self.fetch_glide_html(glide_url)
            available_beers = parse_glide_beers(glide_html)
            enriched = await self.resolve_untappd_matches(available_beers)
        except Exception:
            logging.exception("Beer top refresh failed")
            return None

        grouped = rank_category_entries(enriched)
        if not any(grouped.values()):
            return None

        text = format_beer_message(grouped)
        self._cache_text = text
        self._cache_until = datetime.utcnow() + timedelta(hours=6)
        return text

    async def fetch_channel_html(self) -> str:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.get("https://t.me/s/beerhounds73")
            response.raise_for_status()
            return response.text
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_beer_top_service.py -q`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add beer_top.py tests/test_beer_top_service.py tests/fixtures/glide_listing_sample.html tests/fixtures/untappd_search_sample.html tests/fixtures/untappd_beer_page_sample.html
git commit -m "feat: add beer top enrichment service with fallback"
```

## Task 4: Integrate the beer flow into poll sending and `/top_beer`

**Files:**
- Modify: `bot.py`
- Create: `tests/test_bot_integration.py`

- [ ] **Step 1: Write the failing integration tests**

```python
import pytest

import bot as bot_module


@pytest.mark.asyncio
async def test_send_survey_posts_beer_message_after_two_polls(monkeypatch):
    calls = []

    class FakeBot:
        async def send_poll(self, **kwargs):
            calls.append(("poll", kwargs["question"]))
            return type("Msg", (), {"message_id": len(calls)})()

        async def send_message(self, chat_id, text):
            calls.append(("message", text))

    async def fake_build_message():
        return "Смотри какое интересное пиво я нашел:\n\nIPA\nAlpha"

    monkeypatch.setattr(bot_module, "bot", FakeBot())
    monkeypatch.setattr(bot_module, "build_beer_top_message", fake_build_message)

    await bot_module.send_survey(123)

    assert calls[0][0] == "poll"
    assert calls[1][0] == "poll"
    assert calls[2] == ("message", "Смотри какое интересное пиво я нашел:\n\nIPA\nAlpha")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_bot_integration.py -q`

Expected: FAIL because `send_survey()` does not yet call the beer service or expose `/top_beer`.

- [ ] **Step 3: Write the minimal implementation**

```python
from beer_top import build_beer_top_message


async def send_survey(chat_id: int):
    poll1 = await bot.send_poll(...)
    poll2 = await bot.send_poll(...)
    active_polls[chat_id] = [poll1.message_id, poll2.message_id]

    beer_message = await build_beer_top_message()
    if beer_message:
        await bot.send_message(chat_id=chat_id, text=beer_message)


@dp.message(Command("top_beer"))
async def cmd_top_beer(message: types.Message):
    text = await build_beer_top_message()
    if text is None:
        await message.answer("Пока не получилось собрать подборку пива. Попробуй чуть позже.")
        return
    await message.answer(text)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_bot_integration.py -q`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add bot.py tests/test_bot_integration.py
git commit -m "feat: send beer recommendations after polls"
```

## Task 5: Run full verification and polish operator-facing behavior

**Files:**
- Modify: `bot.py`
- Modify: `beer_top.py`
- Modify: `requirements.txt`
- Modify: `tests/test_beer_top_logic.py`
- Modify: `tests/test_beer_top_parsers.py`
- Modify: `tests/test_beer_top_service.py`
- Modify: `tests/test_bot_integration.py`

- [ ] **Step 1: Add any missing regression tests discovered during integration**

```python
@pytest.mark.asyncio
async def test_send_survey_still_succeeds_when_beer_lookup_fails(monkeypatch):
    ...


def test_rank_category_entries_limits_each_category_to_five_items():
    ...
```

- [ ] **Step 2: Run the full test suite and verify red/green gaps**

Run: `pytest -q`

Expected: PASS with all parser, logic, service, and integration tests green.

- [ ] **Step 3: Polish logging, timeouts, command registration, and dependency list**

```python
await bot.set_my_commands([
    BotCommand(command="start", description="Запустить бота"),
    BotCommand(command="poll", description="Запустить опрос вручную"),
    BotCommand(command="top_beer", description="Показать подборку пива"),
])
```

- [ ] **Step 4: Re-run full verification**

Run: `pytest -q`

Expected: PASS

Optional runtime smoke check after tests:

Run: `python3 -m compileall bot.py beer_top.py`

Expected: no syntax errors

- [ ] **Step 5: Commit**

```bash
git add bot.py beer_top.py requirements.txt tests
git commit -m "feat: add beer top recommendations and command"
```

## Notes for Implementation

- Prefer `httpx.AsyncClient` so the beer feature stays async and does not block the bot event loop.
- Keep network calls behind small helper methods so tests can monkeypatch them cleanly.
- Use reduced HTML fixtures instead of full pages where possible to keep tests readable.
- If Glide parsing turns out to need more than one strategy, add an internal layered parser such as:
  - `parse_glide_from_embedded_json`
  - `parse_glide_from_known_dom`
  - `parse_glide_from_script_payload`
- For Wednesday survey behavior, the beer message is optional; never fail the poll flow if enrichment fails.
- For `/top_beer`, always reply with either the beer message or a short fallback message.
