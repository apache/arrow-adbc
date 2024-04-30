# default read/write/execute_adbc() performs the correct calls

    Code
      db <- adbc_database_init(adbc_driver_log())
    Output
      LogDatabaseNew()
      LogDatabaseInit()
    Code
      try(read_adbc(db, "some sql"))
    Output
      LogConnectionNew()
      LogConnectionInit()
      LogStatementNew()
      LogStatementSetSqlQuery()
      LogStatementRelease()
      LogConnectionRelease()
      Error in adbc_statement_set_sql_query(stmt, query) : NOT_IMPLEMENTED
    Code
      try(execute_adbc(db, "some sql"))
    Output
      LogConnectionNew()
      LogConnectionInit()
      LogStatementNew()
      LogStatementSetSqlQuery()
      LogStatementRelease()
      LogConnectionRelease()
      Error in adbc_statement_set_sql_query(stmt, query) : NOT_IMPLEMENTED
    Code
      try(write_adbc(mtcars, db, "some_table"))
    Output
      LogConnectionNew()
      LogConnectionInit()
      LogStatementNew()
      LogStatementSetOption()
      LogStatementBindStream()
      LogStatementRelease()
      LogConnectionRelease()
      Error in adbc_statement_bind_stream(stmt, tbl) : NOT_IMPLEMENTED
    Code
      adbc_database_release(db)
    Output
      LogDatabaseRelease()

---

    Code
      db <- adbc_database_init(adbc_driver_log())
    Output
      LogDatabaseNew()
      LogDatabaseInit()
    Code
      con <- adbc_connection_init(db)
    Output
      LogConnectionNew()
      LogConnectionInit()
    Code
      try(read_adbc(con, "some sql"))
    Output
      LogStatementNew()
      LogStatementSetSqlQuery()
      LogStatementRelease()
      Error in adbc_statement_set_sql_query(stmt, query) : NOT_IMPLEMENTED
    Code
      try(execute_adbc(con, "some sql"))
    Output
      LogStatementNew()
      LogStatementSetSqlQuery()
      LogStatementRelease()
      Error in adbc_statement_set_sql_query(stmt, query) : NOT_IMPLEMENTED
    Code
      try(write_adbc(mtcars, con, "some_table"))
    Output
      LogStatementNew()
      LogStatementSetOption()
      LogStatementBindStream()
      LogStatementRelease()
      Error in adbc_statement_bind_stream(stmt, tbl) : NOT_IMPLEMENTED
    Code
      adbc_connection_release(con)
    Output
      LogConnectionRelease()
    Code
      adbc_database_release(db)
    Output
      LogDatabaseRelease()

# joiners work for databases, connections, and statements

    Code
      stmt <- local({
        db <- local_adbc(adbc_database_init(adbc_driver_log()))
        con <- local_adbc(adbc_connection_init(db))
        adbc_connection_join(con, db)
        expect_false(adbc_xptr_is_valid(db))
        stmt <- local_adbc(adbc_statement_init(con))
        adbc_statement_join(stmt, con)
        expect_false(adbc_xptr_is_valid(con))
        adbc_xptr_move(stmt)
      })
    Output
      LogDatabaseNew()
      LogDatabaseInit()
      LogConnectionNew()
      LogConnectionInit()
      LogStatementNew()
    Code
      adbc_statement_release(stmt)
    Output
      LogStatementRelease()
      LogConnectionRelease()
      LogDatabaseRelease()

