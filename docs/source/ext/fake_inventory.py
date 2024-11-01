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

"""
Utilities for making fake inventories for non-Sphinx docs.
"""

from __future__ import annotations

import typing

# XXX: we're taking advantage of duck typing to do stupid things here.


class FakeEnv(typing.NamedTuple):
    project: str
    version: str


class FakeObject(typing.NamedTuple):
    # Looks like this
    # name domainname:typ prio uri dispname
    name: str
    # written as '-' if equal to name
    dispname: str
    # member, doc, etc
    typ: str
    # passed through builder.get_target_uri
    docname: str
    # not including the #
    anchor: str
    # written, but never used
    prio: str


class FakeDomain(typing.NamedTuple):
    name: str
    objects: list[FakeObject]

    def get_objects(self):
        return self.objects


class FakeDomainsContainer(typing.NamedTuple):
    domains: list[FakeDomain]

    def sorted(self):
        return self.domains

    @classmethod
    def from_dict(self, domains: dict[str, FakeDomain]) -> FakeDomainsContainer:
        return FakeDomainsContainer(domains=domains.values())


class FakeBuildEnvironment(typing.NamedTuple):
    config: FakeEnv
    domains: FakeDomainsContainer


class FakeBuilder:
    def get_target_uri(self, docname: str) -> str:
        return docname
