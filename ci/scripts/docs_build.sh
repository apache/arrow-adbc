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

    pushd "$source_dir/java"
    mvn site
    popd

    pushd "$source_dir/docs"
    # The project name/version don't really matter here.
    python "$source_dir/docs/source/ext/doxygen_inventory.py" \
           "ADBC C" \
           "version" \
           --html-path "$source_dir/c/apidoc/html" \
           --xml-path "$source_dir/c/apidoc/xml" \
           "cpp/api" \
           "$source_dir/c/apidoc"
    python "$source_dir/docs/source/ext/javadoc_inventory.py" \
           "ADBC Java" \
           "version" \
           "$source_dir/java/target/site/apidocs" \
           "java/api"

    # We need to determine the base URL without knowing it...
    # Inject a dummy URL here, and fix it up in website_build.sh
    export ADBC_INTERSPHINX_MAPPING_java_adbc="http://javadocs.home.arpa/;$source_dir/java/target/site/apidocs/objects.inv"
    export ADBC_INTERSPHINX_MAPPING_cpp_adbc="http://doxygen.home.arpa/;$source_dir/c/apidoc/objects.inv"

    sphinx-build --builder html --nitpicky --fail-on-warning --keep-going source build/html
    rm -rf "$source_dir/docs/build/html/cpp/api"
    cp -r "$source_dir/c/apidoc/html" "$source_dir/docs/build/html/cpp/api"
    rm -rf "$source_dir/docs/build/html/java/api"
    cp -r "$source_dir/java/target/site/apidocs" "$source_dir/docs/build/html/java/api"
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
