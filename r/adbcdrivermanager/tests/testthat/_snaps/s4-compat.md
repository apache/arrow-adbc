# S4 slot definitions work with adbc_* S3 object defined in this package

    Code
      expect_s4_class(obj <- adbc_test_s4_compat(adbc_driver_log()),
      "AdbcS4CompatTest")
    Output
      LogDatabaseNew()
      LogDatabaseInit()
      LogConnectionNew()
      LogConnectionInit()
      LogStatementNew()
      LogDatabaseNew()
      LogDatabaseInit()
      LogConnectionNew()
      LogConnectionInit()
      LogStatementNew()
    Code
      adbc_statement_release(obj@statement)
    Output
      LogStatementRelease()
    Code
      adbc_connection_release(obj@connection)
    Output
      LogConnectionRelease()
    Code
      adbc_database_release(obj@database)
    Output
      LogDatabaseRelease()
      LogDriverRelease()

