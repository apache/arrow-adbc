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

"""A directive for code recipes with a literate-like programming style.

1. Write code recipes as normal, self-contained source files.
2. Add comments for prose containing reStructuredText markup.
3. Use the ``recipe`` directive to include the code in your Sphinx
   documentation. The directive will parse out the prose and render it as
   actual documentation, with the code blocks interspersed.

Effectively, this turns the code "inside out": code with embedded prose
comments will become prose with embedded code blocks.  The actual code remains
valid code and can be tested and run like usual.
"""

from pathlib import Path

import docutils
from docutils.statemachine import StringList
from sphinx.util.docutils import SphinxDirective
from sphinx.util.nodes import nested_parse_with_titles
from sphinx.util.typing import OptionSpec

__all__ = ["setup"]

from . import parser


class RecipeDirective(SphinxDirective):
    has_content = False
    required_arguments = 1
    optional_arguments = 0
    option_spec: OptionSpec = {}

    @staticmethod
    def source_language(filename: str) -> parser.SourceSyntax:
        path = Path(filename)
        language = parser.LANGUAGES.get(path.suffix)
        if not language:
            raise ValueError(f"Unknown language for file {filename}")
        return language

    def run(self):
        rel_filename, filename = self.env.relfn2path(self.arguments[0])
        # Ask Sphinx to rebuild when either the recipe or the directive are changed
        self.env.note_dependency(rel_filename)
        self.env.note_dependency(__file__)

        syntax = self.source_language(filename)
        repo_url_template = self.env.config.recipe_repo_url_template

        with open(filename) as source:
            generated_lines = parser.parse_recipe_to_rest(
                source,
                filename=self.arguments[0],
                rel_filename=rel_filename,
                syntax=syntax,
                repo_url_template=repo_url_template,
            )

        parsed = docutils.nodes.Element()
        nested_parse_with_titles(
            self.state,
            StringList(generated_lines, source=""),
            parsed,
        )
        return parsed.children


def setup(app) -> None:
    app.add_config_value(
        "recipe_repo_url_template",
        default=None,
        rebuild="html",
        types=str,
    )
    app.add_directive("recipe", RecipeDirective)

    return {
        "version": "0.1",
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }
