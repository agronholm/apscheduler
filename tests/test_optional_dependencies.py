from __future__ import annotations

import subprocess
import sys

import pytest


@pytest.mark.parametrize(
    ("module", "dependency", "extra"),
    [
        ("datastores.mongodb", "pymongo", "mongodb"),
        ("datastores.sqlalchemy", "sqlalchemy", "sqlalchemy"),
        ("eventbrokers.asyncpg", "asyncpg", "asyncpg"),
        ("eventbrokers.mqtt", "paho", "mqtt"),
        ("eventbrokers.mqtt", "paho.mqtt", "mqtt"),
        ("eventbrokers.psycopg", "psycopg", "psycopg"),
        ("eventbrokers.redis", "redis", "redis"),
        ("serializers.cbor", "cbor2", "cbor"),
    ],
)
@pytest.mark.parametrize("internal_error", [False, True])
def test_missing_dependency(
    module: str, dependency: str, extra: str, internal_error: bool
) -> None:
    # A fresh interpreter keeps already imported optional dependencies from hiding
    # import failures (the main test suite imports several of these at collection).
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import importlib
import sys
import traceback

module, dependency, extra, internal_error = sys.argv[1:]
name = "internal_dependency" if internal_error == "True" else dependency
error = ModuleNotFoundError(f"No module named {name!r}", name=name, path="original")

class MissingDependencyFinder:
    def find_spec(self, fullname, path=None, target=None):
        if fullname == dependency:
            raise error

sys.meta_path.insert(0, MissingDependencyFinder())
try:
    importlib.import_module(f"apscheduler.{module}")
except ModuleNotFoundError as exc:
    assert exc is error
    assert exc.name == name
    assert exc.path == "original"
    assert str(exc) == f"No module named {name!r}"
    formatted = "".join(traceback.format_exception(exc))
    assert "find_spec" in formatted
    if internal_error == "True":
        assert not getattr(exc, "__notes__", [])
        assert "pip install" not in formatted
    else:
        assert f"pip install 'apscheduler[{extra}]'" in formatted
else:
    raise AssertionError("the dependency import should have failed")
""",
            module,
            dependency,
            extra,
            str(internal_error),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
