# The log driver logs

    Code
      db <- adbc_database_init(adbc_driver_log(), key = "value")
    Output
      LogDatabaseNew()
      LogDatabaseSetOption()
      LogDatabaseInit()
    Code
      try(adbc_database_get_option(db, "key"))
    Output
      LogDatabaseGetOption()
      Error in adbc_database_get_option(db, "key") : NOT_FOUND: Unknown option
    Code
      con <- adbc_connection_init(db, key = "value")
    Output
      LogConnectionNew()
      LogConnectionSetOption()
      LogConnectionInit()
    Code
      try(adbc_connection_get_option(con, "key"))
    Output
      Error in adbc_connection_get_option(con, "key") : 
        NOT_FOUND: Unknown option
    Code
      try(adbc_connection_commit(con))
    Output
      LogConnectionCommit()
      Error in adbc_connection_commit(con) : NOT_IMPLEMENTED
    Code
      try(adbc_connection_get_info(con))
    Output
      LogConnectionGetInfo()
      Error in adbc_connection_get_info(con) : NOT_IMPLEMENTED
    Code
      try(adbc_connection_get_objects(con))
    Output
      LogConnectionGetObjects()
      Error in adbc_connection_get_objects(con) : NOT_IMPLEMENTED
    Code
      try(adbc_connection_get_table_schema(con, NULL, NULL, "table_name"))
    Output
      LogConnectionGetTableSchema()
      Error in adbc_connection_get_table_schema(con, NULL, NULL, "table_name") : 
        NOT_IMPLEMENTED
    Code
      try(adbc_connection_get_table_types(con))
    Output
      LogConnectionGetTableTypes()
      Error in adbc_connection_get_table_types(con) : NOT_IMPLEMENTED
    Code
      try(adbc_connection_read_partition(con, raw()))
    Output
      LogConnectionReadPartition()
      Error in adbc_connection_read_partition(con, raw()) : NOT_IMPLEMENTED
    Code
      try(adbc_connection_rollback(con))
    Output
      LogConnectionRollback()
      Error in adbc_connection_rollback(con) : NOT_IMPLEMENTED
    Code
      try(adbc_connection_cancel(con))
    Output
      LogConnectionCancel()
      Error in adbc_connection_cancel(con) : NOT_IMPLEMENTED
    Code
      try(adbc_connection_get_statistics(con, NULL, NULL, "table_name"))
    Output
      LogConnectionGetStatistics()
      Error in adbc_connection_get_statistics(con, NULL, NULL, "table_name") : 
        NOT_IMPLEMENTED
    Code
      try(adbc_connection_get_statistic_names(con))
    Output
      LogConnectionGetStatisticNames()
      Error in adbc_connection_get_statistic_names(con) : NOT_IMPLEMENTED
    Code
      stmt <- adbc_statement_init(con, key = "value")
    Output
      LogStatementNew()
      LogStatementSetOption()
    Code
      try(adbc_statement_get_option(stmt, "key"))
    Output
      Error in adbc_statement_get_option(stmt, "key") : 
        NOT_FOUND: Unknown option
    Code
      try(adbc_statement_execute_query(stmt))
    Output
      LogStatementExecuteQuery()
      Error in adbc_statement_execute_query(stmt) : NOT_IMPLEMENTED
    Code
      try(adbc_statement_execute_schema(stmt))
    Output
      LogStatementExecuteSchema()
      Error in adbc_statement_execute_schema(stmt) : NOT_IMPLEMENTED
    Code
      try(adbc_statement_prepare(stmt))
    Output
      LogStatementPrepare()
      Error in adbc_statement_prepare(stmt) : NOT_IMPLEMENTED
    Code
      try(adbc_statement_set_sql_query(stmt, ""))
    Output
      LogStatementSetSqlQuery()
      Error in adbc_statement_set_sql_query(stmt, "") : NOT_IMPLEMENTED
    Code
      try(adbc_statement_set_substrait_plan(stmt, raw()))
    Output
      LogStatementSetSubstraitPlan()
      Error in adbc_statement_set_substrait_plan(stmt, raw()) : NOT_IMPLEMENTED
    Code
      try(adbc_statement_bind(stmt, data.frame()))
    Output
      LogStatementBind()
      Error in adbc_statement_bind(stmt, data.frame()) : NOT_IMPLEMENTED
    Code
      try(adbc_statement_bind_stream(stmt, data.frame()))
    Output
      LogStatementBindStream()
      Error in adbc_statement_bind_stream(stmt, data.frame()) : NOT_IMPLEMENTED
    Code
      try(adbc_statement_cancel(stmt))
    Output
      LogStatementCancel()
      Error in adbc_statement_cancel(stmt) : NOT_IMPLEMENTED
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

