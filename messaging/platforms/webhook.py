"""HTTP webhook messaging platform adapter."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

import httpx
from loguru import logger

from ..models import IncomingMessage
from .base import MessagingPlatform


class WebhookPlatform(MessagingPlatform):
    """Messaging adapter for inbound and outbound HTTP webhook integrations."""

    name = "webhook"

    def __init__(
        self,
        *,
        shared_secret: str | None = None,
        outbound_url: str | None = None,
        messaging_rate_limit: int = 1,
        messaging_rate_window: float = 1.0,
    ) -> None:
        self.shared_secret = shared_secret
        self.outbound_url = outbound_url
        self._messaging_rate_limit = messaging_rate_limit
        self._messaging_rate_window = messaging_rate_window
        self._message_handler: Callable[[IncomingMessage], Awaitable[None]] | None = (
            None
        )
        self._client: httpx.AsyncClient | None = None
        self._limiter: Any | None = None
        self._connected = False

    async def start(self) -> None:
        from ..limiter import MessagingRateLimiter

        self._client = httpx.AsyncClient(timeout=30.0)
        self._limiter = await MessagingRateLimiter.get_instance(
            rate_limit=self._messaging_rate_limit,
            rate_window=self._messaging_rate_window,
        )
        self._connected = True
        logger.info("Webhook platform started")

    async def stop(self) -> None:
        if self._client is not None:
            await self._client.aclose()
        self._client = None
        self._connected = False
        logger.info("Webhook platform stopped")

    def on_message(
        self,
        handler: Callable[[IncomingMessage], Awaitable[None]],
    ) -> None:
        self._message_handler = handler

    async def handle_inbound(self, payload: dict[str, Any]) -> str:
        if self._message_handler is None:
            raise RuntimeError("Webhook message handler is not registered")

        message_id = str(payload.get("message_id") or f"webhook_{uuid.uuid4().hex}")
        chat_id = str(payload.get("chat_id") or "webhook")
        user_id = str(payload.get("user_id") or "webhook")
        text = str(payload.get("text") or "")
        reply_to = payload.get("reply_to_message_id")
        thread_id = payload.get("message_thread_id")
        incoming = IncomingMessage(
            text=text,
            chat_id=chat_id,
            user_id=user_id,
            message_id=message_id,
            platform=self.name,
            reply_to_message_id=str(reply_to) if reply_to is not None else None,
            message_thread_id=str(thread_id) if thread_id is not None else None,
            username=str(payload.get("username")) if payload.get("username") else None,
            raw_event=payload,
        )
        await self._message_handler(incoming)
        return message_id

    async def _post_event(self, event_type: str, payload: dict[str, Any]) -> str:
        if not self.outbound_url:
            return str(payload.get("message_id") or f"webhook_{uuid.uuid4().hex}")
        if self._client is None:
            raise RuntimeError("Webhook platform is not initialized")

        body = {"platform": self.name, "event": event_type, **payload}
        headers = {}
        if self.shared_secret:
            headers["X-Webhook-Secret"] = self.shared_secret
        response = await self._client.post(
            self.outbound_url, json=body, headers=headers
        )
        response.raise_for_status()
        try:
            data = response.json()
        except ValueError:
            data = {}
        return str(
            data.get("message_id") or payload.get("message_id") or uuid.uuid4().hex
        )

    async def send_message(
        self,
        chat_id: str,
        text: str,
        reply_to: str | None = None,
        parse_mode: str | None = None,
        message_thread_id: str | None = None,
    ) -> str:
        return await self._post_event(
            "message",
            {
                "chat_id": chat_id,
                "text": text,
                "reply_to_message_id": reply_to,
                "parse_mode": parse_mode,
                "message_thread_id": message_thread_id,
            },
        )

    async def edit_message(
        self,
        chat_id: str,
        message_id: str,
        text: str,
        parse_mode: str | None = None,
    ) -> None:
        await self._post_event(
            "edit",
            {
                "chat_id": chat_id,
                "message_id": message_id,
                "text": text,
                "parse_mode": parse_mode,
            },
        )

    async def delete_message(
        self,
        chat_id: str,
        message_id: str,
    ) -> None:
        await self._post_event(
            "delete",
            {"chat_id": chat_id, "message_id": message_id},
        )

    async def queue_send_message(
        self,
        chat_id: str,
        text: str,
        reply_to: str | None = None,
        parse_mode: str | None = None,
        fire_and_forget: bool = True,
        message_thread_id: str | None = None,
    ) -> str | None:
        async def _send() -> str:
            return await self.send_message(
                chat_id, text, reply_to, parse_mode, message_thread_id
            )

        if self._limiter is None:
            return await _send()
        if fire_and_forget:
            self._limiter.fire_and_forget(_send)
            return None
        return await self._limiter.enqueue(_send)

    async def queue_edit_message(
        self,
        chat_id: str,
        message_id: str,
        text: str,
        parse_mode: str | None = None,
        fire_and_forget: bool = True,
    ) -> None:
        async def _edit() -> None:
            await self.edit_message(chat_id, message_id, text, parse_mode)

        if self._limiter is None:
            await _edit()
        elif fire_and_forget:
            self._limiter.fire_and_forget(
                _edit, dedup_key=f"edit:{chat_id}:{message_id}"
            )
        else:
            await self._limiter.enqueue(_edit, dedup_key=f"edit:{chat_id}:{message_id}")

    async def queue_delete_message(
        self,
        chat_id: str,
        message_id: str,
        fire_and_forget: bool = True,
    ) -> None:
        async def _delete() -> None:
            await self.delete_message(chat_id, message_id)

        if self._limiter is None:
            await _delete()
        elif fire_and_forget:
            self._limiter.fire_and_forget(
                _delete, dedup_key=f"delete:{chat_id}:{message_id}"
            )
        else:
            await self._limiter.enqueue(
                _delete, dedup_key=f"delete:{chat_id}:{message_id}"
            )

    def fire_and_forget(self, task: Awaitable[Any]) -> None:
        if asyncio.iscoroutine(task):
            _ = asyncio.create_task(task)
        else:
            _ = asyncio.ensure_future(task)

    @property
    def is_connected(self) -> bool:
        return self._connected
