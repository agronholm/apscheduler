from __future__ import annotations

from typing import Any

from attrs import Attribute


def positive_number(instance: Any, attribute: Attribute, value: Any) -> None:
    if value <= 0:
        raise ValueError(f"{attribute.name} must be positive, got: {value}")


def non_negative_number(instance: Any, attribute: Attribute, value: Any) -> None:
    if value < 0:
        raise ValueError(f"{attribute.name} must be non-negative, got: {value}")


def aware_datetime(instance: Any, attribute: Attribute, value: Any) -> None:
    if not value.tzinfo:
        raise ValueError(f"{attribute.name} must be a timezone aware datetime")
