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

from typing import Dict

import sphinx.roles
from docutils import nodes
from docutils.parsers.rst import directives
from sphinx import addnodes
from sphinx.directives import ObjectDescription
from sphinx.domains import Domain, Index, ObjType
from sphinx.util.nodes import make_refnode
from sphinx.util.typing import OptionSpec


class JavaPackage(ObjectDescription):
    """Directive to describe a Java package."""

    has_content = True
    required_arguments = 1
    option_spec = {}

    def handle_signature(self, sig, signode):
        prefix = [nodes.Text("package"), addnodes.desc_sig_space()]
        signode += addnodes.desc_annotation(str(prefix), "", *prefix)
        signode += addnodes.desc_name(text=sig)
        return sig

    def add_target_and_index(self, name_cls, sig: str, signode) -> None:
        signode["ids"].append("java:" + sig)
        java_domain = self.env.get_domain("java")
        java_domain.add_item("mod", sig)


class JavaClass(ObjectDescription):
    """Directive to describe a Java class, interface, or exception."""

    has_content = True
    required_arguments = 1

    option_spec: OptionSpec = {
        # TODO: public, private, etc.
        "abstract": directives.flag,
        "final": directives.flag,
        "static": directives.flag,
    }

    def handle_signature(self, sig, signode):
        prefix = []
        # TODO: more modifiers
        for modifier in ["static", "abstract", "final"]:
            if modifier in self.options:
                prefix.extend(
                    [
                        nodes.Text(modifier),
                        addnodes.desc_sig_space(),
                    ]
                )
        prefix.extend([nodes.Text(self.objtype), addnodes.desc_sig_space()])
        signode += addnodes.desc_annotation(str(prefix), "", *prefix)
        signode += addnodes.desc_name(text=sig)
        return sig

    def add_target_and_index(self, name_cls, sig: str, signode) -> None:
        signode["ids"].append("java:" + sig)
        java_domain = self.env.get_domain("java")
        java_domain.add_item(self.objtype, sig)


class JavaMethod(ObjectDescription):
    """Directive to describe a Java method."""

    has_content = True
    required_arguments = 1
    option_spec = {}

    def add_target_and_index(self, name_cls, sig: str, signode) -> None:
        # TODO: validate that the method name is present
        signode["ids"].append("java:" + sig)
        java_domain = self.env.get_domain("java")
        java_domain.add_item("mod", sig)


class PackageIndex(Index):
    """An index of Java packages."""

    name = "packages"
    localname = "Package Index"
    shortname = "Package"

    def generate(self, docnames=None):
        return [], True


class JavaDomain(Domain):
    """A Sphinx Domain for Java."""

    name = "java"
    label = "Java"
    object_types: Dict[str, ObjType] = {
        "package": ObjType("package", "mod"),
    }
    roles = {
        "class": sphinx.roles.XRefRole(),
        "method": sphinx.roles.XRefRole(),
        "mod": sphinx.roles.XRefRole(),
    }
    directives = {
        "class": JavaClass,
        "exc": JavaClass,
        "method": JavaMethod,
        "module": JavaPackage,
    }
    indices = {
        PackageIndex,
    }
    initial_data = {
        "objects": {},
        "mod": {},
    }

    def resolve_any_xref(self, env, fromdocname, builder, target, node, contnode):
        return None

    def resolve_xref(self, env, fromdocname, builder, typ, target, node, contnode):
        match = self.data["mod" if typ == "mod" else "objects"].get(target)
        if match is not None:
            todocname = match[2]
            targ = match[3]
            return make_refnode(builder, fromdocname, todocname, targ, contnode, targ)
        return None

    def add_item(self, typ: str, sig: str) -> None:
        qualified_name = f"java:{sig}"
        anchor = qualified_name
        # dispname, name, type, docname, anchor, priority
        # TODO: namedtuple?
        # TODO: warn if overwrite
        if typ == "mod":
            self.data["mod"][sig] = (
                qualified_name,
                "Package",
                self.env.docname,
                anchor,
                0,
            )
        else:
            # TODO: don't hardcode so much
            self.data["objects"][sig] = (
                qualified_name,
                "Class",
                self.env.docname,
                anchor,
                0,
            )


def setup(app):
    app.add_domain(JavaDomain)
    return {
        "version": "0.1",
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }
