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

# Illiterate Sphinx: Not Literate Programming, For Sphinx

[Literate Programming][literate] is a style of programming introduced by the
venerable Donald Knuth where a program is developed as a prose document
interspersed with code.  The code is then extracted from the document for
actual use.

This isn't that.

Illiterate Sphinx is an extension for the [Sphinx][sphinx] documentation
generator that lets you write code interspersed with special comments.  The
extension then separates the code and comments to generate a prose document
interspersed with code.  Meanwhile, since the source document is still just
code, it can be run and/or tested without special documentation-specific
tooling.  And Sphinx doesn't need to understand anything about the source
language other than what comment delimiter it should look for.

This extension makes it easy to write self-contained "cookbook" documentation
pages that can be run and tested as real code while still being nicely
formatted in documentation.

## Usage

#. Install this extension.
#. Add `illiterate_sphinx` to `extensions` in your `conf.py`.
#. Optionally, set the config value `illiterate_repo_url_template` to a URL
   with a `{rel_filename}` placeholder.  This will be used to link to the full
   recipe source on GitHub or your favorite code hosting platform.
#. Write your cookbook recipe.  For example:

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

#. Include the recipe in your documentation via the `recipe` directive:

    ```rst
    .. recipe:: helloworld.py
    ```

[literate]: https://en.wikipedia.org/wiki/Literate_programming
[sphinx]: https://www.sphinx-doc.org/en/master/
