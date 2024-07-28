from __future__ import annotations

from collections.abc import Callable
from typing import Any

from attrs import Attribute

from apscheduler._utils import unset


def positive_number(instance: Any, attribute: Attribute, value: Any) -> None:
    if value <= 0:
        raise ValueError(f"{attribute.name} must be positive, got: {value}")


def non_negative_number(instance: Any, attribute: Attribute, value: Any) -> None:
    if value < 0:
        raise ValueError(f"{attribute.name} must be non-negative, got: {value}")


def aware_datetime(instance: Any, attribute: Attribute, value: Any) -> None:
    if not value.tzinfo:
        raise ValueError(f"{attribute.name} must be a timezone aware datetime")


def if_not_unset(validator: Callable[[Any, Any, Any], None]) -> None:
    def validate(instance: Any, attribute: Any, value: Any) -> None:
        if value is unset:
            return

        validator(instance, attribute, value)


def valid_metadata(instance: Any, attribute: Attribute, value: Any) -> None:
    def check_value(path: str, val: object) -> None:
        if value is None:
            return

        if isinstance(val, list):
            for index, item in enumerate(val):
                check_value(f"{path}[{index}]", item)
        elif isinstance(val, dict):
            for k, v in val.items():
                if not isinstance(k, str):
                    raise ValueError(f"{path} has a non-string key ({key!r})")

                check_value(f"{path}[{k!r}]", v)
        elif not isinstance(val, (str, int, float, bool)):
            raise ValueError(
                f"{path} has a value that is not JSON compatible: ({val!r})"
            )

    if not isinstance(value, dict):
        raise ValueError(f"{attribute.name} must be a dict, got: {value!r}")

    for key, value in value.items():
        check_value(key, value)
