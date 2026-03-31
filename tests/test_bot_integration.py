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

    async def answer(self, text: str, parse_mode: str | None = None) -> None:
        self.answers.append((text, parse_mode))


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
        ("Пока не получилось собрать подборку пива. Попробуй чуть позже.", None)
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
        ("Пока не получилось собрать подборку пива. Попробуй чуть позже.", None)
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


def test_search_beer_command_sends_search_results(monkeypatch):
    bot_module = load_bot_module(monkeypatch)
    message = FakeMessage("private", "/search_beer ne ipa simcoe до 7 градусов")

    async def fake_build_search_message(query_text: str):
        assert query_text == "ne ipa simcoe до 7 градусов"
        return "Вот что нашел по запросу"

    monkeypatch.setattr(
        bot_module,
        "build_beer_search_message",
        fake_build_search_message,
        raising=False,
    )

    asyncio.run(bot_module.cmd_search_beer(message))

    assert message.answers == [("Вот что нашел по запросу", "HTML")]


def test_search_beer_command_requires_query_text(monkeypatch):
    bot_module = load_bot_module(monkeypatch)
    message = FakeMessage("private", "/search_beer")

    asyncio.run(bot_module.cmd_search_beer(message))

    assert message.answers == [
        (
            "Напиши запрос после команды. Например: /search_beer ne ipa simcoe до 7 градусов",
            None,
        )
    ]
