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

    kind: typing.Literal["source", "prose", "stderr", "stdout"]
    lines: list[SourceLine]


class SourceLanguage(typing.NamedTuple):
    """Language-specific configuration for parsing recipes."""

    #: Language name to use for syntax highlighting.
    pygments_language: str
    #: Prefix for prose comments.
    prose_prefix: str
    #: Prefix for output blocks.
    stdout_prefix: str
    #: Prefix for stderr blocks.
    stderr_prefix: str
    #: Prefix for continuation lines.
    output_continuation_prefix: str


#: Prepended to the Sphinx output to link to the source of the recipe.
PREAMBLE = "Recipe source: `{name} <{url}>`_"
#: Indicates the start of the recipe content.
START = "RECIPE STARTS HERE"
#: Allows you to specify the category (used in the index).
CATEGORY_PREFIX = "RECIPE CATEGORY:"
#: Allows you to specify comma-separated keywords (used in the index).
KEYWORDS_PREFIX = "RECIPE KEYWORDS:"


_LANGUAGES = {
    (".cc", ".cpp"): SourceLanguage(
        pygments_language="cpp",
        prose_prefix="///",
        stdout_prefix="// Output:",
        stderr_prefix="// Standard Error:",
        output_continuation_prefix="//",
    ),
    (".go",): SourceLanguage(
        pygments_language="go",
        prose_prefix="///",
        stdout_prefix="// Output:",
        stderr_prefix="// Standard Error:",
        output_continuation_prefix="//",
    ),
    (".java",): SourceLanguage(
        pygments_language="java",
        prose_prefix="///",
        stdout_prefix="// Output:",
        stderr_prefix="// Standard Error:",
        output_continuation_prefix="//",
    ),
    (".py",): SourceLanguage(
        pygments_language="python",
        prose_prefix="#:",
        stdout_prefix="# Output:",
        stderr_prefix="# Standard Error:",
        output_continuation_prefix="#",
    ),
}
LANGUAGES = {ext: lang for exts, lang in _LANGUAGES.items() for ext in exts}


class RecipeDirective(SphinxDirective):
    has_content = False
    required_arguments = 1
    optional_arguments = 0
    option_spec: OptionSpec = {}

    @staticmethod
    def source_language(filename: str) -> SourceLanguage:
        path = Path(filename)
        language = LANGUAGES.get(path.suffix)
        if not language:
            raise ValueError(f"Unknown language for file {filename}")
        return language

    def run(self):
        rel_filename, filename = self.env.relfn2path(self.arguments[0])
        # Ask Sphinx to rebuild when either the recipe or the directive are changed
        self.env.note_dependency(rel_filename)
        self.env.note_dependency(__file__)

        syntax = self.source_language(filename)

        # --- Split the source into runs of prose or code

        fragments = []
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
                if trimmed.startswith(syntax.prose_prefix):
                    line_type = "prose"
                    # Remove prefix and next whitespace
                    line = trimmed[len(syntax.prose_prefix) + 1 :]
                elif trimmed.startswith(syntax.stdout_prefix):
                    line_type = "stdout"
                    line = trimmed[len(syntax.stdout_prefix) + 1 :]
                elif trimmed.startswith(syntax.stderr_prefix):
                    line_type = "stderr"
                    line = trimmed[len(syntax.stderr_prefix) + 1 :]
                elif fragment_type in ("stdout", "stderr") and trimmed.startswith(
                    syntax.output_continuation_prefix
                ):
                    line = trimmed[len(syntax.output_continuation_prefix) + 1 :]
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

        # --- Split out output fragments, merge adjacent fragments
        # We render output blocks at the end, so remove them here.  Merging
        # adjacent fragments avoids odd breaks in the source code.

        stdout = []
        stderr = []
        new_fragments = []
        for fragment in fragments:
            if fragment.kind == "stdout":
                lines = fragment.lines
                if lines and lines[0].content == "":
                    # Avoid blank line when using format like
                    # // Output:
                    # // theanswer = 42
                    lines = lines[1:]
                stdout.extend(line.content for line in lines)
            elif fragment.kind == "stderr":
                stderr.extend(line.content for line in fragment.lines)
            else:
                if (
                    new_fragments
                    and fragment.kind == "code"
                    and new_fragments[-1].kind == fragment.kind
                ):
                    new_fragments[-1].lines.extend(fragment.lines)
                else:
                    new_fragments.append(fragment)
        fragments = new_fragments

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
                    f"   :language: {syntax.pygments_language}",
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
