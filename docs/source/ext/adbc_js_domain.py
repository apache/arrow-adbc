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

"""Object types for cross-referencing TypeDoc-generated JavaScript API docs."""

import typing

from sphinx.application import Sphinx


def setup(app: Sphinx) -> dict[str, typing.Any]:
    # XXX: despite documentation, these are added to 'std' domain not 'rst'
    # domain (look at the source) — matching what typedoc_inventory.py writes.
    app.add_object_type("jsclass", "jsclass", objname="JavaScript Class")
    app.add_object_type("jsinterface", "jsinterface", objname="JavaScript Interface")
    app.add_object_type("jsenum", "jsenum", objname="JavaScript Enum")
    app.add_object_type(
        "jsenummember", "jsenummember", objname="JavaScript Enum Member"
    )
    app.add_object_type("jsvariable", "jsvariable", objname="JavaScript Variable")
    app.add_object_type("jsfunction", "jsfunction", objname="JavaScript Function")
    app.add_object_type("jstypealias", "jstypealias", objname="JavaScript Type Alias")
    app.add_object_type("jsnamespace", "jsnamespace", objname="JavaScript Namespace")
    app.add_object_type("jsmodule", "jsmodule", objname="JavaScript Module")
    app.add_object_type("jsproperty", "jsproperty", objname="JavaScript Property")
    app.add_object_type("jsmethod", "jsmethod", objname="JavaScript Method")
    app.add_object_type(
        "jsconstructor", "jsconstructor", objname="JavaScript Constructor"
    )
    app.add_object_type("jsaccessor", "jsaccessor", objname="JavaScript Accessor")
    return {
        "version": "0.1",
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }
