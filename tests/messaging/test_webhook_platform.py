from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from messaging.platforms.webhook import WebhookPlatform


@pytest.mark.asyncio
async def test_webhook_handle_inbound_invokes_registered_handler() -> None:
    platform = WebhookPlatform(shared_secret="secret")
    handler = AsyncMock()
    platform.on_message(handler)

    message_id = await platform.handle_inbound(
        {
            "text": "hello",
            "chat_id": "chat-1",
            "user_id": "user-1",
            "message_id": "msg-1",
            "reply_to_message_id": "root",
        }
    )

    assert message_id == "msg-1"
    awaited = handler.await_args
    assert awaited is not None
    incoming = awaited.args[0]
    assert incoming.platform == "webhook"
    assert incoming.text == "hello"
    assert incoming.chat_id == "chat-1"
    assert incoming.user_id == "user-1"
    assert incoming.reply_to_message_id == "root"


@pytest.mark.asyncio
async def test_webhook_send_without_outbound_url_returns_message_id() -> None:
    platform = WebhookPlatform()

    message_id = await platform.send_message("chat-1", "hello")

    assert message_id.startswith("webhook_")
