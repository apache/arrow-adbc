<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# AGENTS.md

You must follow the guidance around pull requests and commits in
@CONTRIBUTING.md.

## Commit Messages and Pull Request Descriptions

Do not repeat the PR title (and do not repeat the Conventional Commits
header).

Be brief. Do not list files changed, and do not summarize changes made unless
there are _significant_ design decisions made.

Do not list tests added, run, or not run, and do not talk about "test
plans". If you did not run a test or were not able to, do not submit the pull
request: the tests must be run, and you are capable of accomplishing this. The
project has limited CI resources and we are not a substitute for your personal
machine. If you added a test and were not able to run it, remove the test.
Note that `git diff`, `pre-commit`, etc. do not count as proper tests. Do not
talk about running or checking those, but make sure that they pass before
committing or submitting.

You must disclose AI usage via the `Generated-by` or `Assisted-by` Git trailer
(because the description will be used as the commit message).

The motivation/problem should be documented by the linked GitHub issue
already; if there is no issue then a very brief description of the rationale
for the change is acceptable.

Do not talk about rebasing, multiple commits, or other branches, repositories,
or pull requests unless they are directly relevant to the content of the pull
request. For example, if you submitted multiple pull requests but they may
conflict, this is not a reason to mention it in the description.

You should perform a code review of your submission before creating a pull
request, and you should ask your human handler to review as well. Submissions
that do not substantatively engage with the maintainers and respect their time
are likely to be closed without review.
