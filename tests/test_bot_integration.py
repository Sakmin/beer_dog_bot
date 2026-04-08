import asyncio
import importlib
import sys
from datetime import datetime
from types import SimpleNamespace


def load_bot_module(monkeypatch):
    monkeypatch.setenv("BOT_TOKEN", "123456:TESTTOKEN")
    sys.modules.pop("bot", None)
    return importlib.import_module("bot")


class FakeMessage:
    def __init__(self, chat_type: str, text: str | None = None) -> None:
        self.chat = SimpleNamespace(id=123, type=chat_type)
        self.text = text
        self.answers: list[tuple[str, str | None, object | None]] = []
        self.documents: list[tuple[bytes, str, str | None]] = []

    async def answer(
        self,
        text: str,
        parse_mode: str | None = None,
        reply_markup: object | None = None,
    ) -> None:
        self.answers.append((text, parse_mode, reply_markup))

    async def answer_document(self, document, caption: str | None = None) -> None:
        self.documents.append((document.data, document.filename, caption))


class FakeCallbackMessage:
    def __init__(self) -> None:
        self.answers: list[tuple[str, str | None, object | None]] = []

    async def answer(
        self,
        text: str,
        parse_mode: str | None = None,
        reply_markup: object | None = None,
    ) -> None:
        self.answers.append((text, parse_mode, reply_markup))


class FakeCallbackQuery:
    def __init__(self, data: str) -> None:
        self.data = data
        self.message = FakeCallbackMessage()
        self.answered = False

    async def answer(self) -> None:
        self.answered = True


def test_send_survey_posts_beer_message_after_two_polls(monkeypatch):
    bot_module = load_bot_module(monkeypatch)
    calls = []

    class FakeBot:
        async def send_poll(self, **kwargs):
            calls.append(("poll", kwargs["question"]))
            return SimpleNamespace(message_id=len(calls))

        async def send_message(self, chat_id, text, parse_mode=None):
            calls.append(("message", chat_id, text, parse_mode))

    async def fake_build_message():
        return "Смотри какое интересное пиво я нашел:\n\nIPA\nAlpha"

    monkeypatch.setattr(bot_module, "bot", FakeBot())
    monkeypatch.setattr(
        bot_module,
        "build_beer_top_message",
        fake_build_message,
        raising=False,
    )

    asyncio.run(bot_module.send_survey(123))

    assert calls == [
        ("poll", "Идем в бар на этой неделе?"),
        ("poll", "Когда тебе удобно?"),
        (
            "message",
            123,
            "Смотри какое интересное пиво я нашел:\n\nIPA\nAlpha",
            "HTML",
        ),
    ]


def test_send_survey_still_sends_two_polls_when_beer_lookup_fails(monkeypatch):
    bot_module = load_bot_module(monkeypatch)
    calls = []

    class FakeBot:
        async def send_poll(self, **kwargs):
            calls.append(("poll", kwargs["question"]))
            return SimpleNamespace(message_id=len(calls))

        async def send_message(self, chat_id, text, parse_mode=None):
            calls.append(("message", chat_id, text, parse_mode))

    async def fake_build_message():
        raise RuntimeError("upstream failed")

    monkeypatch.setattr(bot_module, "bot", FakeBot())
    monkeypatch.setattr(
        bot_module,
        "build_beer_top_message",
        fake_build_message,
        raising=False,
    )

    asyncio.run(bot_module.send_survey(456))

    assert calls == [
        ("poll", "Идем в бар на этой неделе?"),
        ("poll", "Когда тебе удобно?"),
    ]


def test_top_beer_command_sends_recommendation_in_supported_chat_types(monkeypatch):
    bot_module = load_bot_module(monkeypatch)

    async def fake_build_message():
        return "Смотри какое интересное пиво я нашел:\n\nIPA\nAlpha"

    monkeypatch.setattr(
        bot_module,
        "build_beer_top_message",
        fake_build_message,
        raising=False,
    )

    for chat_type in ("private", "group", "supergroup"):
        message = FakeMessage(chat_type)

        asyncio.run(bot_module.cmd_top_beer(message))

        assert message.answers == [
            ("Смотри какое интересное пиво я нашел:\n\nIPA\nAlpha", "HTML", None)
        ]


def test_top_beer_channel_post_handler_sends_recommendation(monkeypatch):
    bot_module = load_bot_module(monkeypatch)
    message = FakeMessage("channel")

    async def fake_build_message():
        return "Смотри какое интересное пиво я нашел:\n\nIPA\nAlpha"

    monkeypatch.setattr(
        bot_module,
        "build_beer_top_message",
        fake_build_message,
        raising=False,
    )

    asyncio.run(bot_module.cmd_top_beer_channel_post(message))

    assert message.answers == [
        ("Смотри какое интересное пиво я нашел:\n\nIPA\nAlpha", "HTML", None)
    ]


def test_top_beer_command_uses_fallback_when_message_is_unavailable(monkeypatch):
    bot_module = load_bot_module(monkeypatch)
    message = FakeMessage("private")

    async def fake_build_message():
        return None

    monkeypatch.setattr(
        bot_module,
        "build_beer_top_message",
        fake_build_message,
        raising=False,
    )

    asyncio.run(bot_module.cmd_top_beer(message))

    assert message.answers == [
        ("Пока нет готового кэша пива. Сначала выполни /refresh_beer_cache.", None, None)
    ]


def test_top_beer_command_uses_fallback_when_builder_raises(monkeypatch):
    bot_module = load_bot_module(monkeypatch)
    message = FakeMessage("group")

    async def fake_build_message():
        raise RuntimeError("upstream failed")

    monkeypatch.setattr(
        bot_module,
        "build_beer_top_message",
        fake_build_message,
        raising=False,
    )

    asyncio.run(bot_module.cmd_top_beer(message))

    assert message.answers == [
        ("Пока нет готового кэша пива. Сначала выполни /refresh_beer_cache.", None, None)
    ]


def test_drink_already_command_sends_filtered_recommendations(monkeypatch):
    bot_module = load_bot_module(monkeypatch)
    message = FakeMessage("private")

    async def fake_build_message():
        return "IPA для старта\n• Bravo Session IPA"

    monkeypatch.setattr(
        bot_module,
        "build_drink_already_message",
        fake_build_message,
        raising=False,
    )

    asyncio.run(bot_module.cmd_drink_already(message))

    assert message.answers == [("IPA для старта\n• Bravo Session IPA", "HTML", None)]


def test_drink_already_command_uses_fallback_when_message_is_unavailable(monkeypatch):
    bot_module = load_bot_module(monkeypatch)
    message = FakeMessage("private")

    async def fake_build_message():
        return None

    monkeypatch.setattr(
        bot_module,
        "build_drink_already_message",
        fake_build_message,
        raising=False,
    )

    asyncio.run(bot_module.cmd_drink_already(message))

    assert message.answers == [
        ("Пока нет готового кэша пива или не удалось прочитать выпитые сорта.", None, None)
    ]


def test_sergey_top_command_sends_personal_top(monkeypatch):
    bot_module = load_bot_module(monkeypatch)
    message = FakeMessage("private")

    async def fake_build_message():
        return "Мой топ пива\n\n• Beer A - Brew A\n🙋 5.00 | ⭐ 4.12 | 👥 120"

    monkeypatch.setattr(
        bot_module,
        "build_sergey_top_message",
        fake_build_message,
        raising=False,
    )

    asyncio.run(bot_module.cmd_sergey_top(message))

    assert message.answers == [
        ("Мой топ пива\n\n• Beer A - Brew A\n🙋 5.00 | ⭐ 4.12 | 👥 120", "HTML", None)
    ]


def test_sergey_top_command_uses_fallback_when_unavailable(monkeypatch):
    bot_module = load_bot_module(monkeypatch)
    message = FakeMessage("private")

    async def fake_build_message():
        return None

    monkeypatch.setattr(
        bot_module,
        "build_sergey_top_message",
        fake_build_message,
        raising=False,
    )

    asyncio.run(bot_module.cmd_sergey_top(message))

    assert message.answers == [
        ("Пока не получилось собрать твой топ пива из Untappd.", None, None)
    ]


def test_send_survey_keeps_polls_when_beer_message_send_fails(monkeypatch):
    bot_module = load_bot_module(monkeypatch)
    calls = []

    class FakeBot:
        async def send_poll(self, **kwargs):
            calls.append(("poll", kwargs["question"]))
            return SimpleNamespace(message_id=len(calls))

        async def send_message(self, chat_id, text, parse_mode=None):
            calls.append(("message", chat_id, text, parse_mode))
            raise RuntimeError("telegram send failed")

    async def fake_build_message():
        return "Смотри какое интересное пиво я нашел:\n\nIPA\nAlpha"

    monkeypatch.setattr(bot_module, "bot", FakeBot())
    monkeypatch.setattr(
        bot_module,
        "build_beer_top_message",
        fake_build_message,
        raising=False,
    )

    asyncio.run(bot_module.send_survey(789))

    assert calls == [
        ("poll", "Идем в бар на этой неделе?"),
        ("poll", "Когда тебе удобно?"),
        (
            "message",
            789,
            "Смотри какое интересное пиво я нашел:\n\nIPA\nAlpha",
            "HTML",
        ),
    ]
    assert bot_module.active_polls[789] == [1, 2]


def test_refresh_beer_cache_command_reports_saved_count(monkeypatch):
    bot_module = load_bot_module(monkeypatch)
    message = FakeMessage("private", "/refresh_beer_cache")

    async def fake_refresh_beer_cache():
        return 42

    monkeypatch.setattr(
        bot_module,
        "refresh_beer_cache",
        fake_refresh_beer_cache,
        raising=False,
    )

    asyncio.run(bot_module.cmd_refresh_beer_cache(message))

    assert message.answers == [("Кэш пива обновлен. Сохранено позиций: 42.", None, None)]


def test_refresh_beer_cache_command_is_private_only(monkeypatch):
    bot_module = load_bot_module(monkeypatch)
    message = FakeMessage("group", "/refresh_beer_cache")

    asyncio.run(bot_module.cmd_refresh_beer_cache(message))

    assert message.answers == [("Эта команда доступна только в личке с ботом.", None, None)]


def test_download_menu_command_sends_cached_menu_as_document(monkeypatch):
    bot_module = load_bot_module(monkeypatch)
    message = FakeMessage("private", "/download_menu")

    def fake_build_menu_download():
        return (
            "beer_menu_2026-04-01.txt",
            "Меню Beer Hounds от 2026-04-01\n\n• Ковбой Мальборо - Plan B",
        )

    monkeypatch.setattr(
        bot_module,
        "build_beer_menu_download",
        fake_build_menu_download,
        raising=False,
    )

    asyncio.run(bot_module.cmd_download_menu(message))

    assert message.answers == []
    assert message.documents == [
        (
            "Меню Beer Hounds от 2026-04-01\n\n• Ковбой Мальборо - Plan B".encode("utf-8"),
            "beer_menu_2026-04-01.txt",
            None,
        )
    ]


def test_download_menu_command_uses_fallback_when_cache_missing(monkeypatch):
    bot_module = load_bot_module(monkeypatch)
    message = FakeMessage("private", "/download_menu")

    monkeypatch.setattr(
        bot_module,
        "build_beer_menu_download",
        lambda: None,
        raising=False,
    )

    asyncio.run(bot_module.cmd_download_menu(message))

    assert message.answers == [
        ("Пока нет готового кэша пива. Сначала выполни /refresh_beer_cache.", None, None)
    ]


def test_hop_guide_command_sends_full_hop_cheatsheet(monkeypatch):
    bot_module = load_bot_module(monkeypatch)
    message = FakeMessage("private", "/hop_guide")

    asyncio.run(bot_module.cmd_hop_guide(message))

    assert len(message.answers) == 1
    text, parse_mode, reply_markup = message.answers[0]
    assert "Как читать это быстро" in text
    assert "<code>Simcoe</code>" in text
    assert "<code>Nelson Sauvin</code>" in text
    assert "<code>Apollo</code>" in text
    assert parse_mode == "HTML"
    assert reply_markup is None


def test_next_cache_refresh_time_returns_next_wednesday_8am_msk(monkeypatch):
    bot_module = load_bot_module(monkeypatch)

    monday = datetime(2026, 4, 6, 10, 30, tzinfo=bot_module.MOSCOW_TZ)
    next_run = bot_module.next_cache_refresh_time(monday)

    assert next_run.weekday() == 2
    assert next_run.hour == 8
    assert next_run.minute == 0


def test_next_cache_refresh_time_rolls_to_next_week_after_wednesday_run_time(monkeypatch):
    bot_module = load_bot_module(monkeypatch)

    late_wednesday = datetime(2026, 4, 8, 9, 0, tzinfo=bot_module.MOSCOW_TZ)
    next_run = bot_module.next_cache_refresh_time(late_wednesday)

    assert next_run.date().isoformat() == "2026-04-15"
    assert next_run.hour == 8
    assert next_run.minute == 0


def test_more_top_command_sends_available_categories_keyboard(monkeypatch):
    bot_module = load_bot_module(monkeypatch)
    message = FakeMessage("private", "/more_top")

    monkeypatch.setattr(
        bot_module,
        "build_more_top_categories",
        lambda: [("IPA для старта", "starter"), ("IPA", "ipa")],
        raising=False,
    )

    asyncio.run(bot_module.cmd_more_top(message))

    assert len(message.answers) == 1
    text, parse_mode, reply_markup = message.answers[0]
    assert text == "Выбери категорию:"
    assert parse_mode is None
    assert reply_markup is not None


def test_more_top_category_callback_sends_extended_category_message(monkeypatch):
    bot_module = load_bot_module(monkeypatch)
    callback = FakeCallbackQuery("more_top:ipa")

    monkeypatch.setattr(
        bot_module,
        "build_more_top_category_message",
        lambda category_key: ("IPA", "Категория IPA\n• One\n• Two"),
        raising=False,
    )

    asyncio.run(bot_module.handle_more_top_category(callback))

    assert callback.answered is True
    assert callback.message.answers == [("Категория IPA\n• One\n• Two", "HTML", None)]
