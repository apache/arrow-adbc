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

# ADBC GLib

ADBC GLib is a wrapper library for ADBC driver manager. ADBC GLib
provides GLib API.

## How to install

```console
$ meson setup --buildtype=release glib.build glib
$ meson compile -C glib.build
$ sudo meson install -C glib.build
```

## How to test

```console
$ (cd glib && bundle install)
$ meson setup glib.build glib
$ BUNDLE_GEMFILE=glib/Gemfile bundle exec meson devenv -C glib.build meson test --print-errorlogs --verbose
```
