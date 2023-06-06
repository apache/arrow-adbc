# The log driver logs

    Code
      db <- adbc_database_init(adbc_driver_log(), key = "value")
    Output
      LogDatabaseNew()
      LogDatabaseSetOption()
      LogDatabaseInit()
    Code
      con <- adbc_connection_init(db, key = "value")
    Output
      LogConnectionNew()
      LogConnectionSetOption()
      LogConnectionInit()
    Code
      stmt <- adbc_statement_init(con, key = "value")
    Output
      LogStatementNew()
      LogStatementSetOption()
    Code
      try(adbc_statement_execute_query(stmt))
    Output
      LogStatementExecuteQuery()
      Error in adbc_statement_execute_query(stmt) : 
        ADBC_STATUS_NOT_IMPLEMENTED (2)
    Code
      adbc_statement_release(stmt)
    Output
      LogStatementRelease()
    Code
      adbc_connection_release(con)
    Output
      LogConnectionRelease()
    Code
      adbc_database_release(db)
    Output
      LogDatabaseRelease()
      LogDriverRelease()

