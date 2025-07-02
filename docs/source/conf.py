# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import os
import sys
from pathlib import Path

sys.path.append(str(Path("./ext/sphinx_recipe").resolve()))
sys.path.append(str(Path("./ext").resolve()))

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "ADBC"
copyright = """2022–2025 The Apache Software Foundation.  Apache Arrow, Arrow,
Apache, the Apache feather logo, and the Apache Arrow project logo are either
registered trademarks or trademarks of The Apache Software Foundation in the
United States and other countries."""
author = "the Apache Arrow Developers"
release = "19"
# Needed to generate version switcher
version = release

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

exclude_patterns = []
extensions = [
    # recipe directive
    "sphinx_recipe",
    # generic directives to enable intersphinx for java
    "adbc_java_domain",
    "numpydoc",
    "sphinx.ext.autodoc",
    "sphinx.ext.doctest",
    "sphinx.ext.intersphinx",
    "sphinx_copybutton",
    "sphinx_design",
    "sphinxext.opengraph",
]
templates_path = ["_templates"]


def on_missing_reference(app, env, node, contnode):
    if str(contnode) in {
        # Polars does something odd with Sphinx such that polars.DataFrame
        # isn't xrefable; suppress the warning.
        "polars.DataFrame",
        # CapsuleType is only in 3.13+
        "CapsuleType",
        # Internal API
        "DbapiBackend",
    }:
        return contnode

    return None


def setup(app):
    app.connect("missing-reference", on_missing_reference)


# -- Options for autodoc ----------------------------------------------------

try:
    import adbc_driver_manager
    import adbc_driver_manager.dbapi  # noqa: F401
except ImportError:
    autodoc_mock_imports = ["adbc_driver_manager"]

try:
    import adbc_driver_postgresql
    import adbc_driver_postgresql.dbapi  # noqa: F401
except ImportError:
    autodoc_mock_imports = ["adbc_driver_postgresql"]

try:
    import adbc_driver_sqlite
    import adbc_driver_sqlite.dbapi  # noqa: F401
except ImportError:
    autodoc_mock_imports = ["adbc_driver_sqlite"]

autodoc_default_options = {
    "show-inheritance": True,
}

# https://stackoverflow.com/questions/11417221/sphinx-autodoc-gives-warning-pyclass-reference-target-not-found-type-warning
nitpick_ignore = [
    ("py:class", "abc.ABC"),
    ("py:class", "datetime.date"),
    ("py:class", "datetime.datetime"),
    ("py:class", "datetime.time"),
    ("py:class", "enum.Enum"),
    ("py:class", "enum.IntEnum"),
]

# -- Options for doctest -----------------------------------------------------

doctest_global_setup = """
try:
    import adbc_driver_sqlite
    import adbc_driver_sqlite.dbapi  # noqa: F401
except ImportError:
    adbc_driver_sqlite = None
"""

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_css_files = [
    "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.1.1/css/all.min.css",
    "css/custom.css",
]
html_static_path = ["_static"]
html_theme = "furo"
html_theme_options = {
    "dark_logo": "logo-dark.png",
    "light_logo": "logo-light.png",
    "source_repository": "https://github.com/apache/arrow-adbc/",
    "source_branch": "main",
    "source_directory": "docs/source/",
}

# -- Options for sphinx-recipe -----------------------------------------------

recipe_repo_url_template = (
    "https://github.com/apache/arrow-adbc/blob/main/docs/source/{rel_filename}"
)

# -- Options for Intersphinx -------------------------------------------------

intersphinx_mapping = {
    "arrow": ("https://arrow.apache.org/docs/", None),
    "pandas": ("https://pandas.pydata.org/docs/", None),
    "polars": ("https://docs.pola.rs/api/python/stable/", None),
    "python": ("https://docs.python.org/3", None),
}

# Add env vars like ADBC_INTERSPHINX_MAPPING_adbc_java = url;path
# to inject more mappings


def _find_intersphinx_mappings():
    prefix = "ADBC_INTERSPHINX_MAPPING_"
    for key, val in os.environ.items():
        if key.startswith(prefix):
            name = key[len(prefix) :]
            url, _, path = val.partition(";")
            print("[ADBC] Found Intersphinx mapping", name)
            intersphinx_mapping[name] = (url, path)


_find_intersphinx_mappings()


# -- Options for numpydoc ----------------------------------------------------

numpydoc_class_members_toctree = False

# -- Options for sphinxext.opengraph -----------------------------------------

if "dev" in release:
    ogp_site_url = "https://arrow.apache.org/adbc/main/"
else:
    ogp_site_url = f"https://arrow.apache.org/adbc/{release}/"
ogp_image = "_static/banner.png"
