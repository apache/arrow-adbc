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

key_value_options <- function(options) {
  if (!is.character(options)) {
    options <- vapply(options, as.character, character(1))
  }

  keys <- names(options)
  if (length(options) == 0) {
    names(options) <- character()
  } else if (is.null(keys) || all(keys == "")) {
    stop("key/value options must be named")
  }

  options
}

xptr_add_option <- function(xptr, key, value) {
  if (is.null(xptr$options)) {
    xptr$options <- new.env(parent = emptyenv())
  }

  xptr$options[[key]] <- unname(value)
}

new_env <- function() {
  new.env(parent = emptyenv())
}

xptr_env <- function(xptr) {
  .Call(RAdbcXptrEnv, xptr)
}

#' @export
length.adbc_xptr <- function(x) {
  length(xptr_env(x))
}

#' @export
names.adbc_xptr <- function(x) {
  names(xptr_env(x))
}


#' @export
`[[.adbc_xptr` <- function(x, i) {
  xptr_env(x)[[i]]
}

#' @export
`[[<-.adbc_xptr` <- function(x, i, value) {
  env <- xptr_env(x)
  env[[i]] <- value
  invisible(x)
}

#' @export
`$.adbc_xptr` <- function(x, name) {
  xptr_env(x)[[name]]
}

#' @export
`$<-.adbc_xptr` <- function(x, name, value) {
  env <- xptr_env(x)
  env[[name]] <- value
  invisible(x)
}

#' @export
print.adbc_xptr <- function(x, ...) {
  str(x, ...)
}

#' @export
str.adbc_xptr <- function(object, ...) {
  cat(sprintf("<%s> %s ", class(object)[1], format(object)))
  env_proxy <- as.list(xptr_env(object))
  env_proxy$options <- as.list(env_proxy$options)
  str(env_proxy, ...)
  invisible(object)
}
