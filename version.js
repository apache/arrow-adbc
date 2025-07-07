const versions = `
0.1.0;0.1.0
0.2.0;0.2.0
0.3.0;0.3.0
0.4.0;0.4.0
0.5.0;0.5.0
0.5.1;0.5.1
0.6.0;0.6.0
0.7.0;0.7.0
0.8.0;0.8.0
0.9.0;0.9.0
0.10.0;0.10.0
0.11.0;0.11.0
13;13
14;14
15;15
16;16
17;17
18;18
19;19
main;20 (dev)
current;19 (current)
`;
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

function adbcInjectVersionSwitcher() {
    // The template should contain this list, we just populate it
    const root = document.querySelector("#version-switcher ul");

    // Variable injected by ci/scripts/website_build.sh
    // Format:
    // path;version\npath2;version2;\n...
    // Versions are sorted at generation time

    const parsedVersions = versions
          .trim()
          .split(/\n/g)
          .map((version) => version.split(/;/))
    // Most recent on top
          .reverse();
    parsedVersions.forEach((version) => {
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

        el.addEventListener("click", (e) => {
            e.preventDefault();
            try {
                let relativePart = window.location.pathname.replace(/^\//, "");
                // Remove the adbc/ prefix
                relativePart = relativePart.replace(/^adbc[^\/]+\//, "");
                // Remove the version number
                relativePart = relativePart.replace(/^[^\/]+\//, "");
                const newUrl = `${el.getAttribute("href")}/${relativePart}`;
                window.fetch(newUrl).then((resp) => {
                    if (resp.status === 200) {
                        window.location.href = newUrl;
                    } else {
                        window.location.href = el.getAttribute("href");
                    }
                }, () => {
                    window.location.href = el.getAttribute("href");
                });
            } catch (e) {
                window.location.href = el.getAttribute("href");
            }
            return false;
        });
    });

    // Inject a banner warning if the user is looking at older/development
    // version documentation

    // If the user has dismissed the popup, don't show it again
    const storageKey = "adbc-ignored-version-warnings";
    const ignoreVersionWarnings = new Set();
    try {
        const savedVersions = JSON.parse(window.localStorage[storageKey]);
        for (const version of savedVersions) {
            ignoreVersionWarnings.add(version);
        }
    } catch (e) {
        // ignore
    }

    if (ignoreVersionWarnings.has(currentVersion)) {
        return;
    }

    let warningBanner = null;
    const redirectUrl = `${versionsRoot}/current`;
    let redirectText = null;
    if (currentVersion.endsWith(" (dev)")) {
        warningBanner = "This is documentation for an unstable development version.";
        redirectText = "Switch to stable version";
    } else {
        const stableVersions = parsedVersions
              .filter(v => v[0] === "current");
        if (stableVersions.length > 0) {
            const stableVersion = stableVersions[0][1].match(/^(.+) \(current\)/)[1];
            if (currentVersion !== stableVersion) {
                warningBanner = `This is documentation for version ${currentVersion}.`;
                redirectText = `Switch to current stable version`;
            }
        }
    }

    if (warningBanner !== null) {
        // Generate on the fly instead of depending on the template containing
        // the right elements/styles
        const container = document.createElement("div");
        const text = document.createElement("span");
        text.innerText = warningBanner + " ";
        const button = document.createElement("a");
        button.setAttribute("href", redirectUrl);
        button.innerText = redirectText;
        const hide = document.createElement("a");
        hide.innerText = "Hide for this version";
        const spacer = document.createTextNode(" ");
        container.appendChild(text);
        container.appendChild(button);
        container.appendChild(spacer);
        container.appendChild(hide);

        hide.addEventListener("click", (e) => {
            container.remove();
            ignoreVersionWarnings.add(currentVersion);
            window.localStorage[storageKey] =
                JSON.stringify(Array.from(ignoreVersionWarnings));
        });

        container.style.background = "#f8d7da";
        container.style.color = "#000";
        container.style.padding = "1em";
        container.style.textAlign = "center";

        document.body.prepend(container);
    }
};

if (document.readyState !== "loading") {
    adbcInjectVersionSwitcher();
} else {
    window.addEventListener("DOMContentLoaded", adbcInjectVersionSwitcher);
}
