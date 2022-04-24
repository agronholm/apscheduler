from __future__ import annotations

import sys

import pytest

from apscheduler.abc import Serializer
from apscheduler.serializers.cbor import CBORSerializer
from apscheduler.serializers.json import JSONSerializer
from apscheduler.serializers.pickle import PickleSerializer

if sys.version_info >= (3, 9):
    from zoneinfo import ZoneInfo
else:
    from backports.zoneinfo import ZoneInfo


@pytest.fixture(scope="session")
def timezone() -> ZoneInfo:
    return ZoneInfo("Europe/Berlin")


@pytest.fixture(
    params=[
        pytest.param(PickleSerializer, id="pickle"),
        pytest.param(CBORSerializer, id="cbor"),
        pytest.param(JSONSerializer, id="json"),
    ]
)
def serializer(request) -> Serializer | None:
    return request.param() if request.param else None


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"
