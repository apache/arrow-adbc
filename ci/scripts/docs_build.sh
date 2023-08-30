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

set -ex

main() {
    local -r source_dir="${1}"

    pushd "$source_dir/c/apidoc"
    doxygen
    popd

    pushd "$source_dir/docs"
    make html
    make doctest
    popd

    for desc_file in $(find "${source_dir}/r" -name DESCRIPTION); do
      local pkg=$(dirname "$desc_file")
      local pkg_name=$(basename $pkg)
      # Only build R documentation for installed packages (e.g., so that
      # Python's documentation build can run without installing the R
      # packages). Packages are installed in ci/scripts/r_build.sh
      if Rscript -e "loadNamespace('$pkg_name')" ; then
        R -e "pkgdown::build_site_github_pages(pkg = '$pkg', dest_dir = '$source_dir/docs/build/html/r/$pkg_name')"
      fi
    done

}

main "$@"
