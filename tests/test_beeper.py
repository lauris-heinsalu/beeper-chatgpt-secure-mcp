from pathlib import Path
from typing import Any

import pytest

from beeper_events_sidecar.beeper import BeeperClient
from beeper_events_sidecar.config import Settings


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        host="127.0.0.1",
        port=23375,
        db_path=tmp_path / "state.sqlite3",
        beeper_base_url="http://127.0.0.1:23374",
        beeper_auth_file=tmp_path / "authorization",
        mcp_bearer_file=tmp_path / "mcp-bearer",
        reconcile_interval_seconds=60,
        reconcile_overlap_seconds=3600,
        reconnect_max_seconds=30,
        delivery_max_attempts=8,
        delivery_base_backoff_seconds=1,
        delivery_max_backoff_seconds=300,
        default_subscription_ttl_seconds=604800,
        secret_rotation_seconds=300,
        callback_verification_cache_seconds=600,
        callback_timeout_seconds=10,
        log_level="INFO",
    )


class RecordingBeeper(BeeperClient):
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.session = None  # type: ignore[assignment]
        self._chat_cache = {}
        self.calls: list[dict[str, str]] = []

    async def _get_json(
        self,
        path: str,
        *,
        params: dict[str, str] | None = None,
        authenticated: bool = True,
    ) -> dict[str, Any]:
        assert path == "/v1/messages/search"
        assert authenticated
        assert params is not None
        self.calls.append(dict(params))

        if "cursor" not in params:
            return {
                "items": [
                    {
                        "id": "newer",
                        "chatID": "chat",
                        "timestamp": "2026-10-06T21:15:00Z",
                    }
                ],
                "hasMore": True,
                "oldestCursor": "older-page",
            }

        return {
            "items": [
                {
                    "id": "older",
                    "chatID": "chat",
                    "timestamp": "2026-10-06T21:14:00Z",
                }
            ],
            "hasMore": False,
        }


@pytest.mark.asyncio
async def test_reconciliation_search_uses_supported_limit_and_paginates(
    tmp_path: Path,
) -> None:
    client = RecordingBeeper(_settings(tmp_path))

    messages = await client.search_messages(
        date_after="2026-10-06T21:00:00Z",
        date_before="2026-10-06T21:20:00Z",
    )

    assert [message["id"] for message in messages] == ["older", "newer"]
    assert len(client.calls) == 2

    first, second = client.calls
    assert first["limit"] == "20"
    assert first["sender"] == "others"
    assert first["excludeLowPriority"] == "false"
    assert first["includeMuted"] == "true"
    assert "cursor" not in first

    assert second["limit"] == "20"
    assert second["cursor"] == "older-page"
    assert second["direction"] == "before"
