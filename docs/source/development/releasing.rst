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

- An `Artifactory`_ API key (log in with your ASF credentials, then generate it from your profile in the upper-right). You must set the Artifactory API key in ``dev/release/.env`` (see ``dev/release/.env.example`` for a template).

- Install ``en_US.UTF-8`` locale. You can confirm available locales by ``locale -a``.
- Install Conda with conda-forge, and create and activate the environment.

  .. code-block::

     mamba create -n adbc -c conda-forge --file ci/conda_env_dev.txt

  This will install two tools used in the release process: ``commitizen`` (generates changelog from commit messages) and ``gh`` (submit jobs/download artifacts).

- Install Docker.

- Clone the main Arrow repository (https://github.com/apache/arrow) and symlink ``arrow-adbc/dev/release/.env`` to ``arrow/dev/release/.env``.  Some release scripts depend on the scripts in the main Arrow repository.

.. _Artifactory: https://apache.jfrog.io

Before creating a Release Candidate
===================================

Regenerate the LICENSE.txt (see CONTRIBUTING.md) and create a pull request if
any changes were needed.

.. code-block::

   # Setup gpg agent for signing artifacts
   source dev/release/setup-gpg-agent.sh

   # Activate conda environment
   mamba activate adbc

Check Nightly Verification Job
------------------------------

Ensure that the `verification job
<https://github.com/apache/arrow-adbc/actions/workflows/nightly-verify.yml>`_
is passing.  This simulates part of the release verification workflow
to detect issues ahead of time.

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

         git switch maint-X.Y.Z
         # Remove the commits that created the changelog and bumped the
         # versions, since 01-source.sh will redo those steps
         git reset --hard HEAD~2
         # Cherry-pick any commits by hand.
         git cherry-pick ...
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
   # so for the first RC this would be: dev/release/01-prepare.sh 3.0.0 4.0.0 5.0.0 0

   dev/release/01-prepare.sh <arrow-dir> <prev-version> <version> <next-version> <rc-number>

   git push -u apache apache-arrow-adbc-<version>-rc<rc-number> maint-<version>

Build source and binaries and submit them
-----------------------------------------

.. code-block::

    # Download the produced source and binaries, sign them, and add the
    # signatures to the GitHub release
    #
    # On macOS the only way I could get this to work was running "echo
    # "UPDATESTARTUPTTY" | gpg-connect-agent" before running this
    # comment otherwise I got errors referencing "ioctl" errors.
    dev/release/02-sign.sh <prev-version> <version> <rc-number>

    # Upload the source release tarball and signs to
    # https://dist.apache.org/repos/dist/dev/arrow .
    dev/release/03-source.sh <version> <rc-number>

    # Upload the Java artifacts
    #
    # Note that you need to press the "Close" button manually by Web interface
    # after you complete the script:
    #   https://repository.apache.org/#stagingRepositories
    dev/release/04-java-upload.sh <arrow-dir> <version> <rc-number>

    # Sign and upload the deb/rpm packages and APT/Yum repositories
    #
    # This reuses release scripts in apache/arrow. So you need to
    # specify cloned apache/arrow directory.
    dev/release/05-linux-upload.sh <arrow-dir> <version> <rc-number>

    # Start verifications for binaries and wheels
    dev/release/06-binary-verify.sh <version> <rc-number>

Verify the Release
------------------

Start the vote thread on dev@arrow.apache.org using the template email from ``06-binary-verify.sh``.

Voting and approval
===================

Start the vote thread on dev@arrow.apache.org and supply instructions for verifying the integrity of the release.
Approval requires a net of 3 +1 votes from PMC members. A release cannot be vetoed.

How to Verify Release Candidates
--------------------------------

#. Install dependencies.  At minimum, you will need:

   - cURL
   - Docker (to verify binaries)
   - Git
   - GnuPG
   - shasum (built into macOS) or sha256sum/sha512sum (on Linux)

   You will also need to install all dependencies to build and verify all languages.
   Roughly, this means:

   - C and C++ compilers (or the equivalent of ``build-essential`` for your platform)
   - Python 3
   - Ruby with headers
      - meson is required
   - bundler, rake, red-arrow, and test-unit Ruby gems
   - GLib and gobject-introspection with headers
      - pkg-config or cmake must be able to find libarrow-glib.so
      - GI_TYPELIB_PATH should be set to the path to the girepository-1.0 directory
   - Java JRE and JDK (Java 8+)
      - the javadoc command must also be accessible
   - Go
   - CMake, ninja-build, libpq (with headers), SQLite (with headers)

   Alternatively, you can have the verification script download and install dependencies automatically via Conda.
   See the environment variables below.

#. Clone the project::

     $ git clone https://github.com/apache/arrow-adbc.git

#. Run the verification script::

     $ cd arrow-adbc
     # Pass the version and the RC number
     $ ./dev/release/verify-release-candidate.sh 0.1.0 6

   These environment variables may be helpful:

   - ``ARROW_TMPDIR=/path/to/directory`` to specify the temporary
     directory used.  Using a fixed directory can help avoid repeating
     the same setup and build steps if the script has to be run
     multiple times.
   - ``USE_CONDA=1`` to download and set up Conda for dependencies.
     In this case, fewer dependencies are required from the system.
     (Git, GnuPG, cURL, and some others are still required.)

#. Once finished and once the script passes, reply to the mailing list
   vote thread with a +1 or a -1.

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
      git push apache apache-arrow-adbc-<version>

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

      # dev/release/post-03-python.sh 0.1.0 0
      dev/release/post-03-python.sh <version> <rc-number>

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

.. dropdown:: Update R packages
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

   This is a manual process.  See the process for the `Arrow R packages
   <https://arrow.apache.org/docs/dev/developers/release.html#post-release-tasks>`_.

.. dropdown:: Upload Ruby packages to RubyGems
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

   You must be one of owners of https://rubygems.org/gems/red-adbc
   . If you aren't an owner of red-adbc yet, an existing owner must
   run the following command line to add you to red-adbc owners:

   .. code-block:: Bash

      gem owner -a ${RUBYGEMS_ORG_ACCOUNT_FOR_RELEASE_MANAGER} red-adbc

   An owner of red-adbc can upload:

   .. code-block:: Bash

      # dev/release/post-06-ruby.sh 1.0.0
      dev/release/post-06-ruby.sh <version>

.. dropdown:: Upload C#/.NET packages to NuGet
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

   You must be one of owners of the package.  If you aren't an owner yet, an
   existing owner can add you at https://nuget.org.

   You will need to [create an API
   key](https://learn.microsoft.com/en-us/nuget/nuget-org/publish-a-package#create-an-api-key).

   An owner can upload:

   .. code-block:: bash

      export NUGET_API_KEY=<your API key here>

      # dev/release/post-07-csharp.sh 1.0.0
      dev/release/post-07-csharp.sh <version>

.. dropdown:: Update conda-forge packages
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

   File a PR that bumps the version to the feedstock:
   https://github.com/conda-forge/arrow-adbc-split-feedstock

   A conda-forge or feedstock maintainer can review and merge.

.. dropdown:: Announce the new release
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

   Write a release announcement (see `example <https://lists.apache.org/thread/6rkjwvyjjfodrxffllh66pcqnp729n3k>`_) and send to announce@apache.org and dev@arrow.apache.org.

   The announcement to announce@apache.org must be sent from your apache.org e-mail address to be accepted.

.. dropdown:: Remove old artifacts
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

   Remove RC artifacts on https://dist.apache.org/repos/dist/dev/arrow/ and old release artifacts on https://dist.apache.org/repos/dist/release/arrow to follow `the ASF policy <https://infra.apache.org/release-download-pages.html#current-and-older-releases>`_:

   .. code-block:: Bash

      dev/release/post-08-remove-old-artifacts.sh

.. dropdown:: Bump versions
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

   This will bump version numbers embedded in files and filenames.

   It will also update the changelog to the newly released changelog.

   .. code-block:: Bash

      # dev/release/post-09-bump-versions.sh ../arrow 0.1.0 0.2.0
      dev/release/post-09-bump-versions.sh <arrow-dir> <version> <next_version>

.. dropdown:: Publish release blog post
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

   Run the script to generate the blog post outline, then fill out the
   outline and create a PR on `apache/arrow-site
   <https://github.com/apache/arrow-site>`_.

   .. code-block:: Bash

      # dev/release/post-10-website.sh ../arrow-site 0.0.0 0.1.0
      dev/release/post-10-website.sh <arrow-site-dir> <prev_version> <version>

.. _nightly-website.yml: https://github.com/apache/arrow-adbc/actions/workflows/nightly-website.yml
