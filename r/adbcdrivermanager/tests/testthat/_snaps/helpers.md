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
      LogDriverRelease()

