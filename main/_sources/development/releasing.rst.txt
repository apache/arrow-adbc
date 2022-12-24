.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at
..
..   http://www.apache.org/licenses/LICENSE-2.0
..
.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

========================
Release Management Guide
========================

This page provides detailed information on the steps followed to perform
a release. It can be used both as a guide to learn the ADBC release
process and as a comprehensive checklist for the Release Manager when
performing a release.

.. seealso::

   `Apache Arrow Release Management Guide
   <https://arrow.apache.org/docs/dev/developers/release.html>`_

Principles
==========

The Apache Arrow Release follows the guidelines defined at the
`Apache Software Foundation Release Policy <https://www.apache.org/legal/release-policy.html>`_.

Preparing for the release
=========================

Some steps of the release require being a committer or a PMC member.

- A GPG key in the Apache Web of Trust to sign artifacts. This will have to be cross signed by other Apache committers/PMC members. You must set your GPG key ID in ``dev/release/.env`` (see ``dev/release/.env.example`` for a template).

- The GPG key needs to be added to this `SVN repo <https://dist.apache.org/repos/dist/dev/arrow/>`_ and `this one <https://dist.apache.org/repos/dist/release/arrow/>`_.
- Configure Maven to `publish artifacts to Apache repositories <http://www.apache.org/dev/publishing-maven-artifacts.html>`_. You will need to `setup a master password <https://maven.apache.org/guides/mini/guide-encryption.html>`_ at ``~/.m2/settings-security.xml`` and ``settings.xml`` as specified on the `Apache guide <http://www.apache.org/dev/publishing-maven-artifacts.html#dev-env>`_. It can be tested with the following command:

  .. code-block::

      # You might need to export GPG_TTY=$(tty) to properly prompt for a passphrase
      mvn clean install -Papache-release

- Install ``en_US.UTF-8`` locale. You can confirm available locales by ``locale -a``.
- Install Conda with conda-forge, and create and activate the environment.

  .. code-block::

     mamba create -n adbc -c conda-forge --file ci/conda_env_dev.txt

  This will install two tools used in the release process: ``commitizen`` (generates changelog from commit messages) and ``gh`` (submit jobs/download artifacts).

Before creating a Release Candidate
===================================

.. code-block::

   # Setup gpg agent for signing artifacts
   source dev/release/setup-gpg-agent.sh

   # Activate conda environment
   mamba activate adbc

Creating a Release Candidate
============================

These are the different steps that are required to create a Release Candidate.

For the initial Release Candidate, we will create a maintenance branch from master.
Follow up Release Candidates will update the maintenance branch by cherry-picking
specific commits.

We have implemented a Feature Freeze policy between Release Candidates.
This means that, in general, we should only add bug fixes between Release Candidates.
In rare cases, critical features can be added between Release Candidates, if
there is community consensus.

Create or update the corresponding maintenance branch
-----------------------------------------------------

.. tab-set::

   .. tab-item:: Initial Release Candidate

      .. code-block::

         # Execute the following from an up to date master branch.
         # This will create a branch locally called maint-X.Y.Z.
         # X.Y.Z corresponds with the Major, Minor and Patch version number
         # of the release respectively. As an example 9.0.0
         git branch maint-X.Y.Z
         # Push the maintenance branch to the remote repository
         git push -u apache maint-X.Y.Z

   .. tab-item:: Follow up Release Candidates

      .. code-block::

         # First cherry-pick any commits by hand.
         git switch maint-X.Y.Z
         git cherry-pick ...
         # Revert the commit that created the changelog so we can
         # regenerate it in 01-source.sh
         git revert <CHANGELOG COMMIT>
         # Push the updated maintenance branch to the remote repository
         git push -u apache maint-X.Y.Z

Create the Release Candidate tag from the updated maintenance branch
--------------------------------------------------------------------

.. code-block::

   # Start from the updated maintenance branch.
   git switch maint-X.Y.Z

   # The following script will create a branch for the Release Candidate,
   # place the necessary commits updating the version number and changelog, and then create a git tag
   # on OSX use gnu-sed with homebrew: brew install gnu-sed (and export to $PATH)
   #
   # <rc-number> starts at 0 and increments every time the Release Candidate is burned
   # so for the first RC this would be: dev/release/01-prepare.sh 4.0.0 5.0.0 0

   dev/release/01-prepare.sh <version> <next-version> <rc-number>

   git push -u apache apache-arrow-adbc-<version>-rc<rc-number>

Build source and binaries and submit them
-----------------------------------------

.. code-block::

    # Build the source release tarball
    dev/release/02-source.sh <version> <rc-number>

    # Download the produced binaries, sign them, and add the
    # signatures to the GitHub release
    #
    # On macOS the only way I could get this to work was running "echo
    # "UPDATESTARTUPTTY" | gpg-connect-agent" before running this
    # comment otherwise I got errors referencing "ioctl" errors.
    dev/release/03-binary-sign.sh <version> <rc-number>

    # Sign and upload the Java artifacts
    #
    # Note that you need to press the "Close" button manually by Web interface
    # after you complete the script:
    #   https://repository.apache.org/#stagingRepositories
    dev/release/04-java-upload.sh <version> <rc-number>

    # Sign and upload the deb/rpm packages and APT/Yum repositories
    #
    # This reuses release scripts in apache/arrow. So you need to
    # specify cloned apache/arrow directory.
    dev/release/05-linux-upload.sh <arrow-dir> <version> <rc-number>

    # Start verifications for binaries and wheels
    dev/release/06-binary-verify.sh <version> <rc-number>

Verify the Release
------------------

Start the vote thread on dev@arrow.apache.org.

Voting and approval
===================

Start the vote thread on dev@arrow.apache.org and supply instructions for verifying the integrity of the release.
Approval requires a net of 3 +1 votes from PMC members. A release cannot be vetoed.

Post-release tasks
==================

After the release vote, we must undertake many tasks to update source artifacts, binary builds, and the Arrow website.

Be sure to go through on the following checklist:

.. dropdown:: Close the GitHub milestone/project
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

   - Open https://github.com/orgs/apache/projects and find the project
   - Click "..." for the project
   - Select "Close"
   - Open https://github.com/apache/arrow-adbc/milestones and find the milestone
   - Click "Close"

.. dropdown:: Add the new release to the Apache Reporter System
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

   Add relevant release data for Arrow to `Apache reporter <https://reporter.apache.org/addrelease.html?arrow>`_.

.. dropdown:: Upload source release artifacts to Subversion
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

   A PMC member must commit the source release artifacts to Subversion:

   .. code-block:: Bash

      # dev/release/post-01-upload.sh 0.1.0 0
      dev/release/post-01-upload.sh <version> <rc-number>
      git push --tag apache apache-arrow-adbc-<version>

.. dropdown:: Create the final GitHub release
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

   A committer must create the final GitHub release:

   .. code-block:: Bash

      # dev/release/post-02-binary.sh 0.1.0 0
      dev/release/post-02-binary.sh <version> <rc-number>

.. dropdown:: Update website
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

   This is done automatically when the tags are pushed. Please check that the
   `nightly-website.yml`_ workflow succeeded.

.. dropdown:: Upload wheels/sdist to PyPI
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

   We use the twine tool to upload wheels to PyPI:

   .. code-block:: Bash

      # dev/release/post-03-python.sh 10.0.0
      dev/release/post-03-python.sh <version>

.. dropdown:: Publish Maven packages
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

   - Logon to the Apache repository: https://repository.apache.org/#stagingRepositories
   - Select the Arrow staging repository you created for RC: ``orgapachearrow-XXXX``
   - Click the ``release`` button

.. dropdown:: Update tags for Go modules
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

   .. code-block:: Bash

      # dev/release/post-04-go.sh 10.0.0
      dev/release/post-04-go.sh <version>

.. dropdown:: Deploy APT/Yum repositories
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

   .. code-block:: Bash

      # This reuses release scripts in apache/arrow. So you need to
      # specify cloned apache/arrow directory.
      #
      # dev/release/post-05-linux.sh ../arrow 10.0.0 0
      dev/release/post-05-linux.sh <arrow-dir> <version> <rc-number>

.. dropdown:: Announce the new release
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

   Write a release announcement (see `example <https://lists.apache.org/thread/6rkjwvyjjfodrxffllh66pcqnp729n3k>`_) and send to announce@apache.org and dev@arrow.apache.org.

   The announcement to announce@apache.org must be sent from your apache.org e-mail address to be accepted.

.. dropdown:: Publish release blog post
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

   TODO

.. dropdown:: Remove old artifacts
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

   Remove RC artifacts on https://dist.apache.org/repos/dist/dev/arrow/ and old release artifacts on https://dist.apache.org/repos/dist/release/arrow to follow `the ASF policy <https://infra.apache.org/release-download-pages.html#current-and-older-releases>`_:

   .. code-block:: Bash

      dev/release/post-06-remove-old-artifacts.sh

.. dropdown:: Bump versions
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

   .. code-block:: Bash

      # dev/release/post-07-bump-versions.sh 0.1.0 0.2.0
      dev/release/post-07-bump-versions.sh <version> <next_version>

.. _nightly-website.yml: https://github.com/apache/arrow-adbc/actions/workflows/nightly-website.yml
