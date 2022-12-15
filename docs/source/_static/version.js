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

// Generate a version switcher on the fly.  This is actually meant to
// be loaded from the website root (arrow.apache.org/adbc/version.js)
// and not from the documentation subdirectory itself.  This lets us
// update the script globally.  It depends on certain variables being
// injected into the Sphinx template.

window.addEventListener("DOMContentLoaded", () => {
    // The template should contain this list, we just populate it
    const root = document.querySelector("#version-switcher ul");

    // Variable injected by ci/scripts/website_build.sh
    // Format:
    // path;version\npath2;version2;\n...
    // Versions are sorted at generation time
    window.versions
        .trim()
        .split(/\n/g)
        .map((version) => version.split(/;/))
        .forEach((version) => {
            const el = document.createElement("a");
            // Variable injected by template
            el.setAttribute("href", versionsRoot + "/" + version[0]);
            el.innerText = version[1];
            if (version[1] === currentVersion) {
                el.classList.toggle("active");
            }
            const li = document.createElement("li");
            li.appendChild(el);
            root.appendChild(li);
        });
});
