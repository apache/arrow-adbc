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

adbc_allocate_error <- function(shelter = NULL) {
  .Call(RAdbcAllocateError, shelter)
}

stop_for_error <- function(status, error) {
  if (!identical(status, 0L)) {
    msg <- if (!is.null(error$message)) error$message else .Call(RAdbcStatusCodeMessage, status)
    stop(msg)
  }
}

#' @export
print.adbc_error <- function(x, ...) {
  str(x, ...)
}

#' @importFrom utils str
#' @export
str.adbc_error <- function(object, ...) {
  cat("<adbc_error> ")
  str(.Call(RAdbcErrorProxy, object), ...)
  invisible(object)
}

# This is the list()-like interface that allows $ and [[
# to make nice auto-complete when interacting in an IDE

#' @export
length.adbc_error <- function(x, ...) {
  3L
}

#' @export
names.adbc_error <- function(x, ...) {
  c("message", "vendor_code", "sqlstate")
}

#' @export
`[[.adbc_error` <- function(x, i, ...) {
  .Call(RAdbcErrorProxy, x)[[i]]
}

#' @export
`$.adbc_error` <- function(x, i, ...) {
  x[[i]]
}
