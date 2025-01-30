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

import typing
from pathlib import Path

import docutils
from docutils.statemachine import StringList
from sphinx.util.docutils import SphinxDirective
from sphinx.util.nodes import nested_parse_with_titles
from sphinx.util.typing import OptionSpec

__all__ = ["setup"]


class SourceLine(typing.NamedTuple):
    """A reference into the recipe source file."""

    content: str
    #: 1-indexed.  Used for proper line numbers for code blocks.
    lineno: int


class SourceFragment(typing.NamedTuple):
    """A run of source or prose lines in a recipe."""

    kind: typing.Literal["source", "prose"]
    lines: list[SourceLine]


#: Prepended to the Sphinx output to link to the source of the recipe.
PREAMBLE = "Recipe source: `{name} <{url}>`_"
START = "RECIPE STARTS HERE"
CATEGORY_PREFIX = "RECIPE CATEGORY:"
KEYWORDS_PREFIX = "RECIPE KEYWORDS:"


class RecipeDirective(SphinxDirective):
    has_content = False
    required_arguments = 1
    optional_arguments = 0
    option_spec: OptionSpec = {}

    @staticmethod
    def default_language(filename: str) -> str:
        path = Path(filename)
        language = {
            ".cpp": "cpp",
            ".cc": "cpp",
            ".go": "go",
            ".java": "java",
            ".py": "python",
        }.get(path.suffix)

        if not language:
            raise ValueError(f"Unknown language for file {filename}")
        return language

    @staticmethod
    def default_prose_prefix(language: str) -> str:
        return {
            "cpp": "///",
            "go": "///",
            "java": "///",
            "python": "#:",
        }.get(language, "#:")

    @staticmethod
    def default_output_prefix(language: str, output: str) -> str:
        return {
            "cpp": f"// {output}:",
            "go": f"// {output}:",
            "java": f"// {output}:",
            "python": f"# {output}:",
        }.get(language, f"# {output}:")

    def run(self):
        rel_filename, filename = self.env.relfn2path(self.arguments[0])
        # Ask Sphinx to rebuild when either the recipe or the directive are changed
        self.env.note_dependency(rel_filename)
        self.env.note_dependency(__file__)

        language = self.default_language(filename)
        prefix = self.default_prose_prefix(language)
        stderr_prefix = self.default_output_prefix(language, "STDERR")
        stdout_prefix = self.default_output_prefix(language, "STDOUT")

        # --- Split the source into runs of prose or code

        fragments = []
        stdout = []
        stderr = []
        category = None
        keywords = []

        fragment = []
        fragment_type = None
        # "before" --> ignore code lines (e.g. for a license header)
        # "reading" --> parse code lines
        state = "before"
        lineno = 1
        for line in open(filename):
            if state == "before":
                if START in line:
                    state = "reading"
                elif CATEGORY_PREFIX in line:
                    index = line.find(CATEGORY_PREFIX)
                    category = line[index + len(CATEGORY_PREFIX) :].strip()
                elif KEYWORDS_PREFIX in line:
                    index = line.find(KEYWORDS_PREFIX)
                    keywords = [
                        keyword.strip()
                        for keyword in line[index + len(KEYWORDS_PREFIX) :]
                        .strip()
                        .split(",")
                    ]
            elif state == "reading":
                trimmed = line.lstrip()
                if trimmed.startswith(prefix):
                    line_type = "prose"
                    # Remove prefix and next whitespace
                    line = trimmed[len(prefix) + 1 :]
                elif trimmed.startswith(stdout_prefix):
                    line = trimmed[len(stdout_prefix) + 1 :]
                    stdout.append(line)
                    lineno += 1
                    continue
                elif trimmed.startswith(stderr_prefix):
                    line = trimmed[len(stderr_prefix) + 1 :]
                    stderr.append(line)
                    lineno += 1
                    continue
                else:
                    line_type = "code"

                if line_type != fragment_type:
                    if fragment:
                        fragments.append(
                            SourceFragment(kind=fragment_type, lines=fragment)
                        )
                        fragment = []
                    fragment_type = line_type

                # Skip blank code lines (blank lines in reST are significant)
                if line_type != "code" or line.strip():
                    # Remove trailing newline
                    fragment.append(SourceLine(content=line[:-1], lineno=lineno))

            lineno += 1

        if fragment:
            fragments.append(SourceFragment(kind=fragment_type, lines=fragment))

        # --- Generate the final reST as a whole and parse it
        # That way, section hierarchy works properly

        generated_lines = []

        if category and keywords:
            generated_lines.append(".. index::")
            for keyword in keywords:
                generated_lines.append(f"   pair: {category}; {keyword} (recipe)")
            generated_lines.append("")

        # Link to the source on GitHub
        repo_url_template = self.env.config.recipe_repo_url_template
        if repo_url_template is not None:
            repo_url = repo_url_template.format(rel_filename=rel_filename)
            generated_lines.append(
                PREAMBLE.format(
                    name=Path(rel_filename).name,
                    url=repo_url,
                )
            )

        # Paragraph break
        generated_lines.append("")

        for fragment in fragments:
            if fragment.kind == "prose":
                generated_lines.extend([line.content for line in fragment.lines])
                generated_lines.append("")
            elif fragment.kind == "code":
                line_min = fragment.lines[0].lineno
                line_max = fragment.lines[-1].lineno
                lines = [
                    f".. literalinclude:: {self.arguments[0]}",
                    f"   :language: {language}",
                    "   :linenos:",
                    "   :lineno-match:",
                    f"   :lines: {line_min}-{line_max}",
                    "",
                ]
                generated_lines.extend(lines)
            else:
                raise RuntimeError(f"Unknown fragment kind {fragment.kind}")

        if stdout:
            generated_lines.append(".. code-block:: text")
            generated_lines.append("   :caption: stdout")
            generated_lines.append("")
            for line in stdout:
                # reST escapes the content of a code-block directive
                generated_lines.append("   " + line)
            generated_lines.append("")

        if stderr:
            generated_lines.append(".. code-block:: text")
            generated_lines.append("   :caption: stderr")
            generated_lines.append("")
            for line in stderr:
                generated_lines.append("   " + line)
            generated_lines.append("")

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
