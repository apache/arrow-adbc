#!/usr/bin/env bash
#
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

# https://apache.org/legal/release-policy#publication
# Don't link to GitHub, etc.
set -ue

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${SOURCE_DIR}/utils-common.sh"
source "${SOURCE_DIR}/utils-prepare.sh"

main() {
    set_resolved_issues "${RELEASE}"

    read -p "Enter the URL of the announcement blog post..." BLOG_URL

    cat <<EOF
To: announce@apache.org
CC: dev@arrow.apache.org
Subject: [ANNOUNCE] Apache Arrow ADBC ${RELEASE} Released

The Apache Arrow PMC is pleased to announce release ${RELEASE} of the
Apache Arrow ADBC subproject. Individual components are versioned
separately: some packages are on version ${VERSION_NATIVE} and others
are on version ${VERSION_JAVA}.

Release artifacts can be downloaded from:
https://arrow.apache.org/adbc/current/driver/installation.html

This release contains ${RESOLVED_ISSUES} bug fixes and improvements.
Release notes are available at:
${BLOG_URL}

What is Apache Arrow?
---------------------
Apache Arrow is a columnar in-memory analytics layer designed to
accelerate big data. It houses a set of canonical in-memory
representations of flat and hierarchical data along with multiple
language-bindings for structure manipulation. It also provides
low-overhead streaming and batch messaging, zero-copy interprocess
communication (IPC), and vectorized in-memory analytics
libraries. Languages currently supported include C, C++, C#, Go, Java,
JavaScript, Julia, MATLAB, Python, R, Ruby, and Rust.

What is Apache Arrow ADBC?
--------------------------
ADBC is a database access abstraction for Arrow-based applications. It
provides a cross-language API for working with databases while using
Arrow data, providing an alternative to APIs like JDBC and ODBC for
analytical applications. For more, see [1].

Please report any feedback to the mailing lists ([2], [3]).

Regards,
The Apache Arrow Community

[1]: https://arrow.apache.org/blog/2023/01/05/introducing-arrow-adbc/
[2]: https://lists.apache.org/list.html?user@arrow.apache.org
[3]: https://lists.apache.org/list.html?dev@arrow.apache.org
EOF

}

main "$@"
