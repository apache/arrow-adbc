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

"""A basic Java domain for Sphinx."""

import typing

from sphinx.application import Sphinx


def setup(app: Sphinx) -> dict[str, typing.Any]:
    # XXX: despite documentation, this is added to 'std' domain not 'rst'
    # domain (look at the source)
    app.add_object_type(
        "javatype",
        "jtype",
        objname="Java Type",
    )
    app.add_object_type(
        "javamember",
        "jmember",
        objname="Java Member",
    )
    app.add_object_type(
        "javapackage",
        "jpackage",
        objname="Java Package",
    )
    return {
        "version": "0.1",
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }
