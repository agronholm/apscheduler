from __future__ import annotations

from datetime import datetime
from typing import Any

from attrs import Attribute

from ._exceptions import DeserializationError
from .abc import Trigger


def as_int(value) -> int | None:
    """Convert the value into an integer."""
    if value is None:
        return None

    return int(value)


def positive_number(instance, attribute, value) -> None:
    if value <= 0:
        raise ValueError(f"Expected positive number, got {value} instead")


def non_negative_number(instance, attribute, value) -> None:
    if value < 0:
        raise ValueError(f"Expected non-negative number, got {value} instead")


def as_positive_integer(value, name: str) -> int:
    if isinstance(value, int):
        if value > 0:
            return value
        else:
            raise ValueError(f"{name} must be positive")

    raise TypeError(
        f"{name} must be an integer, got {value.__class__.__name__} instead"
    )


def aware_datetime(instance: Any, attribute: Attribute, value: datetime) -> None:
    if not value.tzinfo:
        raise ValueError(f"{attribute.name} must be a timezone aware datetime")


def require_state_version(
    trigger: Trigger, state: dict[str, Any], max_version: int
) -> None:
    try:
        if state["version"] > max_version:
            raise DeserializationError(
                f"{trigger.__class__.__name__} received a serialized state with "
                f'version {state["version"]}, but it only supports up to version '
                f"{max_version}. This can happen when an older version of APScheduler "
                f"is being used with a data store that was previously used with a "
                f"newer APScheduler version."
            )
    except KeyError as exc:
        raise DeserializationError(
            'Missing "version" key in the serialized state'
        ) from exc
