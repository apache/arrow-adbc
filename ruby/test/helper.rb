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

require "adbc"

require "tempfile"

require "test-unit"

module Helper
  def require_gi_bindings(major, minor, micro)
    return if GLib.check_binding_version?(major, minor, micro)
    message =
      "Require gobject-introspection #{major}.#{minor}.#{micro} or later: " +
      GLib::BINDING_VERSION.join(".")
    omit(message)
  end
end
