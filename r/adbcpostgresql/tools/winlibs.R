# Link against libpq static libraries
VERSION <- commandArgs(TRUE)
if(!file.exists(sprintf("../windows/libpq-%s/include/libpq-fe.h", VERSION))){
  if(getRversion() < "3.3.0") setInternet2()
  download.file(sprintf("https://github.com/rwinlib/libpq/archive/v%s.zip", VERSION), "lib.zip", quiet = TRUE)
  dir.create("../windows", showWarnings = FALSE)
  unzip("lib.zip", exdir = "../windows")
  unlink("lib.zip")
}
