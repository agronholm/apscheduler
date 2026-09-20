# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

from __future__ import annotations

# -- Path setup --------------------------------------------------------------
# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
# import os
# import sys
# sys.path.insert(0, os.path.abspath('.'))


# -- Project information -----------------------------------------------------

project = "APScheduler"
copyright = "Alex Grönholm"
author = "Alex Grönholm"


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx",
    "sphinx_tabs.tabs",
    "sphinx_autodoc_typehints",
    "sphinx_rtd_theme",
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []

autodoc_default_options = {"members": True}
autodoc_mock_imports = [
    "asyncpg",
    "bson",
    "cbor2",
    "paho",
    "pymongo",
    "psycopg",
    "redis",
    "sqlalchemy",
    "PyQt6",
]
autodoc_type_aliases = {
    "datetime": "datetime.datetime",
    "UUID": "uuid.UUID",
    "AsyncEngine": "sqlalchemy.ext.asyncio.AsyncEngine",
    "RetrySettings": "apscheduler.RetrySettings",
    "Serializer": "apscheduler.abc.Serializer",
}
nitpick_ignore = [("py:class", "datetime")]

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "sphinx_rtd_theme"

intersphinx_mapping = {
    "python": ("https://docs.python.org/3/", None),
    "anyio": ("https://anyio.readthedocs.io/en/latest/", None),
    "asyncpg": ("https://magicstack.github.io/asyncpg/current/", None),
    "cbor2": ("https://cbor2.readthedocs.io/en/latest/", None),
    "psycopg": ("https://www.psycopg.org/psycopg3/docs/", None),
    "pymongo": ("https://pymongo.readthedocs.io/en/stable", None),
    "sqlalchemy": ("https://docs.sqlalchemy.org/en/20/", None),
    "tenacity": ("https://tenacity.readthedocs.io/en/latest/", None),
}
