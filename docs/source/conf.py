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

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "Apache Arrow ADBC"
copyright = "2022, Apache Arrow Developers"
author = "the Apache Arrow Developers"
release = "1.0.0a0"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

exclude_patterns = []
extensions = [
    "breathe",
    "numpydoc",
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx",
    "sphinx_copybutton",
    "sphinx_design",
]
templates_path = ["_templates"]

# -- Options for autodoc ----------------------------------------------------

try:
    import adbc_driver_manager
    import adbc_driver_manager.dbapi  # noqa: F401
except ImportError:
    autodoc_mock_imports = ["adbc_driver_manager"]

autodoc_default_options = {
    "show-inheritance": True,
}

# -- Options for Breathe -----------------------------------------------------

breathe_default_project = "adbc"
breathe_projects = {
    "adbc": "../../c/apidoc/xml/",
}

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_logo = "_static/logo.png"
html_theme = "sphinx_book_theme"
html_theme_options = {
    "path_to_docs": "docs",
    "repository_branch": "main",
    "repository_url": "https://github.com/apache/arrow-adbc",
    "use_edit_page_button": True,
    "use_issues_button": True,
    "use_repository_button": True,
}
html_static_path = ["_static"]

# -- Options for Intersphinx -------------------------------------------------

intersphinx_mapping = {
    "arrow": ("https://arrow.apache.org/docs/", None),
}

# -- Options for numpydoc ----------------------------------------------------

numpydoc_class_members_toctree = False
