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
#
set -ue

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${SOURCE_DIR}/utils-common.sh"
source "${SOURCE_DIR}/utils-prepare.sh"

main() {

    set_resolved_issues "${RELEASE}"

    cat <<EOF
To: announce@apache.org
CC: dev@arrow.apache.org
Subject: [ANNOUNCE] Apache Arrow ADBC ${RELEASE} Released

The Apache Arrow community is pleased to announce the a new release of
the Apache Arrow ADBC libraries. It includes ${RESOLVED_ISSUES} resolved GitHub issues
([1]). Individual components are versioned separately: some packages
are on version ${VERSION_NATIVE} and others are now version ${VERSION_JAVA}, with the
release as a whole on version '${RELEASE}'.

The release is available now from [2] and [3].

Release notes are available at:
https://github.com/apache/arrow-adbc/blob/apache-arrow-adbc-${RELEASE}/CHANGELOG.md

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
analytical applications. For more, see [4].

Please report any feedback to the mailing lists ([5], [6]).

Regards,
The Apache Arrow Community

[1]: https://github.com/apache/arrow-adbc/issues?q=is%3Aissue+milestone%3A%22ADBC+Libraries+${RELEASE}%22+is%3Aclosed
[2]: https://arrow.apache.org/adbc/current/driver/installation.html
[3]: https://apache.jfrog.io/ui/native/arrow
[4]: https://arrow.apache.org/blog/2023/01/05/introducing-arrow-adbc/
[5]: https://lists.apache.org/list.html?user@arrow.apache.org
[6]: https://lists.apache.org/list.html?dev@arrow.apache.org
EOF

}

main "$@"
