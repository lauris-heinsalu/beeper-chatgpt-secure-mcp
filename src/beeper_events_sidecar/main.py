from __future__ import annotations

import asyncio
import json
import logging
import signal
from datetime import UTC, datetime

import aiohttp
from aiohttp import web

from .beeper import BeeperClient
from .config import Settings
from .db import Database
from .mcp import McpApi
from .service import EventService
from .webhooks import CallbackHttpClient, WebhookSender


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "time": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def configure_logging(level: str) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)


async def run() -> None:
    settings = Settings.from_env()
    configure_logging(settings.log_level)
    logger = logging.getLogger(__name__)

    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        db = Database(settings.db_path)
        beeper = BeeperClient(settings, session)
        callback_client = CallbackHttpClient(
            timeout_seconds=settings.callback_timeout_seconds
        )
        webhook_sender = WebhookSender(callback_client)
        service = EventService(
            settings=settings,
            db=db,
            beeper=beeper,
            webhook_sender=webhook_sender,
        )
        api = McpApi(
            settings=settings,
            db=db,
            service=service,
            webhook_sender=webhook_sender,
        )

        await service.start()
        runner = web.AppRunner(
            api.application(),
            access_log=None,
        )
        await runner.setup()
        site = web.TCPSite(runner, settings.host, settings.port)
        await site.start()
        logger.info(
            "Beeper Events sidecar listening on http://%s:%d/mcp",
            settings.host,
            settings.port,
        )

        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop.set)
            except NotImplementedError:
                pass

        try:
            await stop.wait()
        finally:
            logger.info("Stopping Beeper Events sidecar")
            await runner.cleanup()
            await service.stop()


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
