#!/usr/bin/env python3
from __future__ import annotations

from importlib.metadata import version as get_version

from packaging.version import parse

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx",
    "sphinx_rtd_theme",
]

templates_path = ["_templates"]
source_suffix = ".rst"
master_doc = "index"
project = "APScheduler"
author = "Alex Grönholm"
copyright = "2009, " + author

v = parse(get_version("APScheduler"))
version = v.base_version
release = v.public

language = "en"

exclude_patterns = ["_build"]
pygments_style = "sphinx"
autodoc_default_options = {"members": True, "show-inheritance": True}
autodoc_mock_imports = [
    "bson",
    "gevent",
    "kazoo",
    "pymongo",
    "PyQt6",
    "redis",
    "rethinkdb",
    "sqlalchemy",
    "tornado",
    "twisted",
]
todo_include_todos = False
html_theme = "sphinx_rtd_theme"

intersphinx_mapping = {
    "python": ("https://docs.python.org/3/", None),
    "sqlalchemy": ("https://docs.sqlalchemy.org/en/20/", None),
}
