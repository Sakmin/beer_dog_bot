import asyncio
import importlib
import sys
from types import SimpleNamespace


def load_bot_module(monkeypatch):
    monkeypatch.setenv("BOT_TOKEN", "123456:TESTTOKEN")
    sys.modules.pop("bot", None)
    return importlib.import_module("bot")


class FakeMessage:
    def __init__(self, chat_type: str, text: str | None = None) -> None:
        self.chat = SimpleNamespace(id=123, type=chat_type)
        self.text = text
        self.answers: list[tuple[str, str | None]] = []
        self.documents: list[tuple[bytes, str, str | None]] = []

    async def answer(self, text: str, parse_mode: str | None = None) -> None:
        self.answers.append((text, parse_mode))

    async def answer_document(self, document, caption: str | None = None) -> None:
        self.documents.append((document.data, document.filename, caption))


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
            ("Смотри какое интересное пиво я нашел:\n\nIPA\nAlpha", "HTML")
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
        ("Смотри какое интересное пиво я нашел:\n\nIPA\nAlpha", "HTML")
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
        ("Пока нет готового кэша пива. Сначала выполни /refresh_beer_cache.", None)
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
        ("Пока нет готового кэша пива. Сначала выполни /refresh_beer_cache.", None)
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

    assert message.answers == [("Кэш пива обновлен. Сохранено позиций: 42.", None)]


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
        ("Пока нет готового кэша пива. Сначала выполни /refresh_beer_cache.", None)
    ]
