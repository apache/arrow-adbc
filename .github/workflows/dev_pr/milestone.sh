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

# Assign a milestone to the given PR based on the open milestones and known
# releases.

set -euo pipefail

main() {
    local -r repo="${1}"
    local -r pr_number="${2}"
    echo "On ${repo} pull ${pr_number}"

    local -r existing_milestone=$(gh pr view "${pr_number}" \
                                  --json milestone \
                                  -t '{{if .milestone}}{{.milestone.title}}{{end}}')

    if [[ -n "${existing_milestone}" ]]; then
        echo "PR has milestone: ${existing_milestone}"
        local -r milestone="${existing_milestone}"
    else
        local -r latest_version=$(git ls-remote --heads origin |
                                      grep -o '[0-9.]*$' |
                                      sort --version-sort |
                                      tail -n1)

        local -r milestone=$(gh api "/repos/${repo}/milestones" |
                                 jq -r '.[] | .title' |
                                 grep -E '^ADBC Libraries' |
                                 grep -v "${latest_version}" |
                                 head -n1)

        echo "Latest tagged version: ${latest_version}"
        echo "Assigning milestone: ${milestone}"

        gh pr edit "${pr_number}" -m "${milestone}"
    fi

    local -r repo_owner=$(echo "${repo}" | cut -d'/' -f1)
    local -r repo_name=$(echo "${repo}" | cut -d'/' -f2)
    local -r graphql_query="{
      repository(owner: \"${repo_owner}\", name: \"${repo_name}\") {
        pullRequest(number: ${pr_number}) {
          closingIssuesReferences(first: 5) {
            edges {
              node {
                number
              }
            }
          }
        }
      }
    }"
    local -r linked_issues=$(gh api graphql -f query="${graphql_query}" | jq -r '.data.repository.pullRequest.closingIssuesReferences.edges | .[].node.number')
    for issue in ${linked_issues}; do
        echo "Linked issue: ${issue}"
        gh issue edit "${issue}" --milestone "${milestone}"
    done
}

main "$@"
