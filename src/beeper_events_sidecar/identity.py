"""Versioned, unambiguous identity for one source occurrence.

Source-instance labels identify a *logical source*, not a VM or process.
The same source must retain its label across deployments and restarts.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SourceIdentity:
    source_system: str
    source_instance: str
    account_id: str
    chat_id: str
    message_id: str

    def __post_init__(self) -> None:
        if any(
            not isinstance(value, str) or not value
            for value in (
                self.source_system,
                self.source_instance,
                self.account_id,
                self.chat_id,
                self.message_id,
            )
        ):
            raise ValueError("Every source identity component must be a non-empty string")

    @property
    def canonical(self) -> str:
        return json.dumps(
            [
                "source-occurrence",
                2,
                self.source_system,
                self.source_instance,
                self.account_id,
                self.chat_id,
                self.message_id,
            ],
            ensure_ascii=False,
            separators=(",", ":"),
        )

    @property
    def digest(self) -> str:
        return hashlib.sha256(self.canonical.encode("utf-8")).hexdigest()

    @property
    def source_key(self) -> str:
        return f"source:v2:{self.digest}"

    @property
    def source_event_id(self) -> str:
        return f"src_v2_{self.digest}"
