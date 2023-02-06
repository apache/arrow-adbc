#!/usr/bin/env bash
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

set -eu

main() {
    if [ "$#" -ne 3 ]; then
        echo "Usage: $0 <arrow-site-checkout> <previous-version> <version>"
        exit 1
    fi
    local -r arrow_site="$1"
    local -r prev_version="$2"
    local -r version="$3"

    local -r source_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    local -r source_top_dir="$( cd "${source_dir}/../../" && pwd )"

    export SOURCE_DIR="${source_dir}"
    source "${source_top_dir}/dev/release/utils-prepare.sh"

    # Extract ADBC spec version from Doxygen macro in header.
    local -r spec_version=$(grep '[\]version' "${source_top_dir}/adbc.h" | awk '{print $NF}')
    local -r date=${POST_DATE:-$(date "+%Y-%m-%d")}
    local -r filename="${arrow_site}/_posts/${date}-adbc-${version}-release.md"
    local -r contributor_command="git shortlog --perl-regexp --author='^((?!dependabot\[bot\]).*)$' -sn apache-arrow-adbc-${prev_version}..apache-arrow-adbc-${version}"
    local -r contributor_list=$(eval "${contributor_command}")
    local -r contributors=$(echo "${contributor_list}" | wc -l)

    set_resolved_issues "${version}"

    cat <<EOF | tee "${filename}"
---
layout: post
title: "Apache Arrow ADBC ${version} (Libraries) Release"
date: "${date} 00:00:00"
author: pmc
categories: [release]
---
<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->

The Apache Arrow team is pleased to announce the ${version} release of
the Apache Arrow ADBC libraries. This covers includes [**${RESOLVED_ISSUES}
resolved issues**][1] from [**${contributors} distinct contributors**][2].

This is a release of the **libraries**, which are at version
${version}.  The **API specification** is versioned separately and is
at version ${spec_version}.

The release notes below are not exhaustive and only expose selected
highlights of the release. Many other bugfixes and improvements have
been made: we refer you to the [complete changelog][3].

## Release Highlights

<!-- TODO: fill this portion in. -->

## Contributors

\`\`\`
\$ ${contributor_command}
${contributor_list}
\`\`\`

## Roadmap

<!-- TODO: fill this portion in. -->

## Getting Involved

We welcome questions and contributions from all interested.  Issues
can be filed on [GitHub][4], and questions can be directed to GitHub
or the [Arrow mailing lists][5].

[1]: ${MILESTONE_URL}
[2]: #contributors
[3]: https://github.com/apache/arrow-adbc/blob/apache-arrow-adbc-${version}/CHANGELOG.md
[4]: https://github.com/apache/arrow-adbc/issues
[5]: {% link community.md %}
EOF

}

main "$@"
