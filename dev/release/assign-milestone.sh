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

# Assign the milestone on PRs/issues.  This used to be done by CI, but since
# we can no longer use pull_request_target workflows, do it manually instead.

set -euo pipefail

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${SOURCE_DIR}/utils-common.sh"

main() {
    # Scan the Git log for PRs since the last release
    local -r commits=$(git log --abbrev-commit --pretty=oneline apache-arrow-adbc-${PREVIOUS_RELEASE}..)
    local -r pr_numbers=$(echo "${commits}" | grep -E -o ' \(#[0-9]+\)$' | grep -E -o '[0-9]+')

    local -r milestone=$(gh api graphql \
                            --paginate \
                            --jq ".data.repository.milestones.nodes.[]" \
                            -f query="{
  repository(owner: \"apache\", name: \"arrow-adbc\") {
    milestones(states: [OPEN], first: 10, query: \"ADBC Libraries ${RELEASE}\") {
      nodes {
        id
        title
      }
    }
  }
}")
    local -r milestone_title=$(echo "${milestone}" | jq -r '.title')

    echo "Milestone to use: ${milestone_title}"
    read -p "Press ENTER to continue..." ignored

    for pr_number in ${pr_numbers}; do
        local pr=$(gh api graphql \
                      --paginate \
                      --jq ".data.repository.pullRequest" \
                      -f query="{
  repository(owner: \"apache\", name: \"arrow-adbc\") {
    pullRequest(number: ${pr_number}) {
      title
      milestone {
        title
      }
      closingIssuesReferences(first: 10) {
        nodes {
          number
          milestone {
            title
          }
        }
      }
    }
  }
}")

        local existing_milestone=$(echo "${pr}" | jq -r '.milestone.title')
        if [[ "${existing_milestone}" != "null" ]]; then
            echo "#${pr_number}: has milestone \`${existing_milestone}\`"
        else
            echo "#${pr_number}: assigning ${milestone_title}"
            gh pr edit "${pr_number}" --milestone "${milestone_title}"
            sleep 0.3
        fi

        echo "${pr}" | jq -c '.closingIssuesReferences.nodes.[]' | while read -r issue; do
            local issue_number=$(echo "${issue}" | jq -r '.number')
            local issue_milestone=$(echo "${issue}" | jq -r '.milestone.title')
            if [[ "${issue_milestone}" != "null" ]]; then
                echo "    linked issue #${issue_number}: has milestone \`${issue_milestone}\`"
            else
                echo "    linked issue #${issue_number}: assigning ${milestone_title}"
                gh issue edit "${issue_number}" --milestone "${milestone_title}"
                sleep 0.3
            fi
        done

        sleep 0.3
    done
}

main "$@"
