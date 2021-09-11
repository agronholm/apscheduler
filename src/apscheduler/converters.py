from __future__ import annotations

from datetime import datetime
from typing import Optional
from uuid import UUID


def as_aware_datetime(value: datetime | str) -> Optional[datetime]:
    """Convert the value from a string to a timezone aware datetime."""
    if isinstance(value, str):
        # fromisoformat() does not handle the "Z" suffix
        if value.upper().endswith('Z'):
            value = value[:-1] + '+00:00'

        value = datetime.fromisoformat(value)

    return value


def as_uuid(value: UUID | str) -> UUID:
    """Converts a string-formatted UUID to a UUID instance."""
    if isinstance(value, str):
        return UUID(value)

    return value
