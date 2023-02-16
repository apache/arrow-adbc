
download.file(
  "https://www.sqlite.org/2022/sqlite-amalgamation-3400100.zip",
  "tools/sqlite.zip",
  mode = "wb"
)

unzip("tools/sqlite.zip", exdir = "tools")
src_dir <- list.files("tools", "sqlite-amalgamation-", full.names = TRUE)
stopifnot(length(src_dir) == 1)

suppressWarnings(file.remove(file.path("tools", c("sqlite3.h", "sqlite3.c"))))
file.copy(
  file.path(src_dir, c("sqlite3.h", "sqlite3.c")),
  "tools"
)

unlink(src_dir, recursive = TRUE)
unlink("tools/sqlite.zip")
