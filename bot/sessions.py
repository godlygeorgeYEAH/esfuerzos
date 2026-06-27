"""
sessions.py - In-memory session store for the Reune WhatsApp bot.

Each Session tracks one phone number through the intake conversation.
Sessions expire after ttl_seconds of inactivity (default 1 hour).

Replace the in-memory dict with a Redis backend when scaling beyond
a single process -- the interface (get/set/delete) stays the same.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


class BotState(str, enum.Enum):
    IDLE = "IDLE"
    AWAITING_ROLE = "AWAITING_ROLE"
    # Missing person reporter flow
    MISSING_NAME = "MISSING_NAME"
    MISSING_AGE = "MISSING_AGE"
    MISSING_LOCATION = "MISSING_LOCATION"
    MISSING_MARKS = "MISSING_MARKS"
    MISSING_PHOTO = "MISSING_PHOTO"
    MISSING_CONFIRM = "MISSING_CONFIRM"
    # Found person reporter flow
    FOUND_NAME = "FOUND_NAME"
    FOUND_AGE = "FOUND_AGE"
    FOUND_LOCATION = "FOUND_LOCATION"
    FOUND_STATE = "FOUND_STATE"
    FOUND_PHOTO = "FOUND_PHOTO"
    FOUND_CONFIRM = "FOUND_CONFIRM"
    # Search flow
    SEARCH_QUERY = "SEARCH_QUERY"


@dataclass
class Session:
    phone: str
    state: BotState = BotState.IDLE
    data: dict = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class SessionStore:
    """
    Thread-safe-enough in-memory store for single-process asyncio deployments.
    The GIL protects dict mutations; no additional locking needed for CPython.
    """

    def __init__(self) -> None:
        self._store: dict[str, Session] = {}

    def get(self, phone: str) -> Optional[Session]:
        return self._store.get(phone)

    def set(self, phone: str, session: Session) -> None:
        session.updated_at = datetime.now(timezone.utc)
        self._store[phone] = session

    def delete(self, phone: str) -> None:
        self._store.pop(phone, None)

    def cleanup_expired(self, ttl_seconds: int = 3600) -> None:
        """Remove sessions that have been idle longer than ttl_seconds."""
        now = datetime.now(timezone.utc)
        expired = [
            phone
            for phone, s in self._store.items()
            if (now - s.updated_at).total_seconds() > ttl_seconds
        ]
        for phone in expired:
            del self._store[phone]

    def __len__(self) -> int:
        return len(self._store)


# Module-level singleton used by flows and webhook_router.
store = SessionStore()
