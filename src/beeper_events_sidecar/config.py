from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return default if value is None else int(value)


@dataclass(frozen=True, slots=True)
class Settings:
    host: str
    port: int
    db_path: Path
    beeper_base_url: str
    beeper_auth_file: Path
    mcp_bearer_file: Path
    reconcile_interval_seconds: int
    reconcile_overlap_seconds: int
    reconnect_max_seconds: int
    delivery_max_attempts: int
    delivery_base_backoff_seconds: int
    delivery_max_backoff_seconds: int
    default_subscription_ttl_seconds: int
    secret_rotation_seconds: int
    callback_verification_cache_seconds: int
    callback_timeout_seconds: int
    log_level: str
    delivery_concurrency: int = 4
    mcp_bearer_refresh_seconds: int = 2

    @classmethod
    def from_env(cls) -> Settings:
        home = Path.home()
        state_dir = Path(
            os.getenv(
                "BEEPER_EVENTS_STATE_DIR",
                str(home / ".local" / "share" / "beeper-events-sidecar"),
            )
        )
        return cls(
            host=os.getenv("BEEPER_EVENTS_HOST", "127.0.0.1"),
            port=_env_int("BEEPER_EVENTS_PORT", 23375),
            db_path=Path(
                os.getenv(
                    "BEEPER_EVENTS_DB_PATH",
                    str(state_dir / "state.sqlite3"),
                )
            ),
            beeper_base_url=os.getenv(
                "BEEPER_EVENTS_BEEPER_BASE_URL",
                "http://127.0.0.1:23374",
            ).rstrip("/"),
            beeper_auth_file=Path(
                os.getenv(
                    "BEEPER_EVENTS_BEEPER_AUTH_FILE",
                    str(
                        home
                        / ".config"
                        / "openai-tunnel"
                        / "beeper-authorization"
                    ),
                )
            ),
            mcp_bearer_file=Path(
                os.getenv(
                    "BEEPER_EVENTS_MCP_BEARER_FILE",
                    str(
                        home
                        / ".config"
                        / "beeper-events-sidecar"
                        / "mcp-bearer"
                    ),
                )
            ),
            reconcile_interval_seconds=_env_int(
                "BEEPER_EVENTS_RECONCILE_INTERVAL_SECONDS", 60
            ),
            reconcile_overlap_seconds=_env_int(
                "BEEPER_EVENTS_RECONCILE_OVERLAP_SECONDS", 3600
            ),
            reconnect_max_seconds=_env_int(
                "BEEPER_EVENTS_RECONNECT_MAX_SECONDS", 30
            ),
            delivery_max_attempts=_env_int(
                "BEEPER_EVENTS_DELIVERY_MAX_ATTEMPTS", 12
            ),
            delivery_base_backoff_seconds=_env_int(
                "BEEPER_EVENTS_DELIVERY_BASE_BACKOFF_SECONDS", 30
            ),
            delivery_max_backoff_seconds=_env_int(
                "BEEPER_EVENTS_DELIVERY_MAX_BACKOFF_SECONDS", 21_600
            ),
            default_subscription_ttl_seconds=_env_int(
                "BEEPER_EVENTS_DEFAULT_SUBSCRIPTION_TTL_SECONDS",
                7 * 24 * 60 * 60,
            ),
            secret_rotation_seconds=_env_int(
                "BEEPER_EVENTS_SECRET_ROTATION_SECONDS", 300
            ),
            callback_verification_cache_seconds=_env_int(
                "BEEPER_EVENTS_CALLBACK_CACHE_SECONDS", 600
            ),
            callback_timeout_seconds=_env_int(
                "BEEPER_EVENTS_CALLBACK_TIMEOUT_SECONDS", 10
            ),
            log_level=os.getenv("BEEPER_EVENTS_LOG_LEVEL", "INFO").upper(),
            delivery_concurrency=max(
                1,
                _env_int("BEEPER_EVENTS_DELIVERY_CONCURRENCY", 4),
            ),
            mcp_bearer_refresh_seconds=max(
                0,
                _env_int("BEEPER_EVENTS_MCP_BEARER_REFRESH_SECONDS", 2),
            ),
        )
