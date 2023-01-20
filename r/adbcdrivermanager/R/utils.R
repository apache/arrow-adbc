
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
  x
}

#' @export
`$.adbc_xptr` <- function(x, name) {
  xptr_env(x)[[name]]
}

#' @export
`$<-.adbc_xptr` <- function(x, name, value) {
  env <- xptr_env(x)
  env[[name]] <- value
  x
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
