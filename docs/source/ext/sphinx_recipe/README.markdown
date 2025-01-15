<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# sphinx-recipe: Not Literate Programming, For Sphinx

[Literate Programming][literate] is a style of programming introduced by the
venerable Donald Knuth where a program is developed as a prose document
interspersed with code.  The code is then extracted from the document for
actual use.

This isn't that.

sphinx-recipe is an extension for the [Sphinx][sphinx] documentation generator
that lets you write code interspersed with special comments.  The extension
then separates the code and comments to generate a prose document interspersed
with code.  Meanwhile, since the source document is still just code, it can be
run and/or tested without special documentation-specific tooling.  And Sphinx
doesn't need to understand anything about the source language other than what
comment delimiter it should look for.

This extension makes it easy to write self-contained "cookbook" documentation
pages that can be run and tested as real code while still being nicely
formatted in documentation.

## Usage

1. Install this extension.

   This extension can be installed via pip from this repo:

   ```bash
   pip install 'git+https://github.com/apache/arrow-adbc@main#subdirectory=docs/source/ext/sphinx_recipe'
   pip install 'https://github.com/apache/arrow-adbc/archive/main.zip#subdirectory=docs/source/ext/sphinx_recipe'
   ```

   The latter is likely a bit faster (it downloads the source archive vs
   having to actually perform a Git clone).

1. Add `sphinx_recipe` to `extensions` in your `conf.py`.
1. Optionally, set the config value `recipe_repo_url_template` to a URL
   with a `{rel_filename}` placeholder.  This will be used to link to the full
   recipe source on GitHub or your favorite code hosting platform.
1. Write your cookbook recipe.  For example:

    ```python

    # This is my license header. It's boring, so I don't want it to show up
    # in the documentation.

    # RECIPE STARTS HERE
    #: Some prose describing what I'm about to do.
    #: **reStructuredText syntax works here.**
    print("Hello, world!")

    #: I can have more prose now.
    #:
    #: - I can even write lists.
    print("Goodbye, world!")
    ```

1. Include the recipe in your documentation via the `recipe` directive:

    ```rst
    .. recipe:: helloworld.py
    ```

[literate]: https://en.wikipedia.org/wiki/Literate_programming
[sphinx]: https://www.sphinx-doc.org/en/master/
