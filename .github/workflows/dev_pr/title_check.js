// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

const fs = require("fs");

const COMMIT_TYPES = [
    'build',
    'chore',
    'ci',
    'docs',
    'feat',
    'fix',
    'perf',
    'refactor',
    'revert',
    'style',
    'test',
];

const COMMENT_BODY = ":warning: Please follow the [Conventional Commits format in CONTRIBUTING.md](https://github.com/apache/arrow-adbc/blob/main/CONTRIBUTING.md) for PR titles.";

function matchesCommitFormat(title) {
    const commitType = `(?:${COMMIT_TYPES.join('|')})`;
    const scope = "(?:\\(([a-zA-Z0-9_/\\-,]+)\\))?";
    const delimiter = "!?:";
    const subject = " .+";
    const regexp = new RegExp(`^${commitType}${scope}${delimiter}${subject}$`);

    const matches = title.match(regexp);
    if (matches === null) {
        return false;
    } else if (typeof matches[1] === "undefined") {
        // No component
        return true;
    }

    const components = matches[1].split(",");
    console.info(`Components are ${components}`);
    for (const component of components) {
        if (component === "format") {
            console.info(`Component is "format"`);
            continue;
        } else if (!fs.existsSync(component)) {
            console.info(`Component "${component}" does not exist!`);
            return false;
        }
        console.info(`Component "${component}" is valid`);
    }

    return true;
}

async function commentCommitFormat(github, context, pullRequestNumber) {
    const {data: comments} = await github.rest.issues.listComments({
        owner: context.repo.owner,
        repo: context.repo.repo,
        issue_number: pullRequestNumber,
        per_page: 100,
    });

    let found = false;
    for (const comment of comments) {
        if (comment.body.includes("Conventional Commits format in CONTRIBUTING.md")) {
            found = true;
            break;
        }
    }

    if (!found) {
        await github.rest.issues.createComment({
            owner: context.repo.owner,
            repo: context.repo.repo,
            issue_number: pullRequestNumber,
            body: COMMENT_BODY,
        });
    }
}

module.exports = async ({github, context}) => {
    const pullRequestNumber = context.payload.number;
    const title = context.payload.pull_request.title;
    console.info(`Checking title "${title}"`);
    if (!matchesCommitFormat(title)) {
        console.info(`Title was not valid`);
        await commentCommitFormat(github, context, pullRequestNumber);
    }
};
