
curl::curl_download(
  "https://github.com/apache/arrow-adbc/archive/refs/heads/main.zip",
  "data-raw/adbc.zip"
)

unzip("data-raw/adbc.zip", exdir = "data-raw")
file.rename("data-raw/arrow-adbc-main", "data-raw/arrow-adbc")
unlink("data-raw/adbc.zip")

# replace the adbc.h header
unlink("src/adbc.h")
file.copy("data-raw/arrow-adbc/adbc.h", "src/adbc.h")

# copy the driver manager implementation to src/
file.copy(
  c(
    "data-raw/arrow-adbc/c/driver_manager/adbc_driver_manager.cc",
    "data-raw/arrow-adbc/c/driver_manager/adbc_driver_manager.h"
  ),
  "src"
)
