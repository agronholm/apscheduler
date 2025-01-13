"""This module contains several handy functions primarily meant for internal use."""

from __future__ import annotations

from datetime import datetime, tzinfo
from typing import TYPE_CHECKING, Any, NoReturn, TypeVar
from zoneinfo import ZoneInfo

from ._exceptions import DeserializationError
from .abc import Trigger

if TYPE_CHECKING:
    from ._structures import MetadataType

T = TypeVar("T")


class UnsetValue:
    """The type of :data:`unset`."""

    __slots__ = ()

    def __new__(cls) -> UnsetValue:
        try:
            return unset
        except NameError:
            return super().__new__(cls)

    def __getstate__(self) -> NoReturn:
        raise RuntimeError("Internal error: attempted to serialize an unset value")

    def __repr__(self) -> str:
        return "<unset>"


unset = UnsetValue()


def timezone_repr(timezone: tzinfo) -> str:
    if isinstance(timezone, ZoneInfo):
        return timezone.key
    else:
        return repr(timezone)


def absolute_datetime_diff(dateval1: datetime, dateval2: datetime) -> float:
    return dateval1.timestamp() - dateval2.timestamp()


def qualified_name(cls: type) -> str:
    module = getattr(cls, "__module__", None)
    if module is None or module == "builtins":
        return cls.__qualname__
    else:
        return f"{module}.{cls.__qualname__}"


def require_state_version(
    trigger: Trigger, state: dict[str, Any], max_version: int
) -> None:
    try:
        if state["version"] > max_version:
            raise DeserializationError(
                f"{trigger.__class__.__name__} received a serialized state with "
                f"version {state['version']}, but it only supports up to version "
                f"{max_version}. This can happen when an older version of APScheduler "
                f"is being used with a data store that was previously used with a "
                f"newer APScheduler version."
            )
    except KeyError as exc:
        raise DeserializationError(
            'Missing "version" key in the serialized state'
        ) from exc


def merge_metadata(
    base_metadata: MetadataType, *overlays: MetadataType | UnsetValue
) -> MetadataType:
    new_metadata = base_metadata.copy()
    for metadata in overlays:
        if isinstance(metadata, UnsetValue):
            continue

        new_metadata.update(metadata)

    return new_metadata


def create_repr(instance: object, *attrnames: str, **kwargs) -> str:
    kv_pairs: list[tuple[str, object]] = []
    for attrname in attrnames:
        value = getattr(instance, attrname)
        if value is not unset and value is not None:
            kv_pairs.append((attrname, value))

    for key, value in kwargs.items():
        if value is not unset and value is not None:
            kv_pairs.append((key, value))

    rendered_attrs = ", ".join(f"{key}={value!r}" for key, value in kv_pairs)
    return f"{instance.__class__.__name__}({rendered_attrs})"
