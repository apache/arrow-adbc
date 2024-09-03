// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <stdio.h>
#include <string.h>
#include <chrono>
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#define ERRMSG_LEN 200

SQLINTEGER checkError(SQLRETURN rc, SQLSMALLINT handleType, SQLHANDLE handle,
                      SQLWCHAR* errmsg) {
  SQLRETURN retcode = SQL_SUCCESS;

  SQLSMALLINT errNum = 1;
  SQLCHAR sqlState[6];  // always exactly 5 characters + NUL
  SQLINTEGER nativeError;
  SQLCHAR errMsg[ERRMSG_LEN];
  SQLSMALLINT textLengthPtr;

  if ((rc != SQL_SUCCESS) && (rc != SQL_SUCCESS_WITH_INFO) && (rc != SQL_NO_DATA)) {
    SQLLEN numRecs = 0;
    SQLGetDiagField(SQL_HANDLE_STMT, handle, 0, SQL_DIAG_NUMBER, &numRecs, 0, 0);
    while (retcode != SQL_NO_DATA) {
      retcode = SQLGetDiagRecA(handleType, handle, errNum, sqlState, &nativeError, errMsg,
                               ERRMSG_LEN, &textLengthPtr);

      if (retcode == SQL_INVALID_HANDLE) {
        std::cerr << "checkError function was called with an invalid handle!!"
                  << std::endl;
        return 1;
      }

      if ((retcode != SQL_SUCCESS) && (retcode != SQL_SUCCESS_WITH_INFO)) {
        wprintf(L"ERROR: %d: %ls : %ls\n", nativeError, sqlState, errMsg);
      }

      errNum++;
    }

    wprintf(L"%ls\n", errmsg);
    return 1;
  }

  return 0;
}

#define CHECK_OK(EXPR, ERROR)                                        \
  do {                                                               \
    auto ret = (EXPR);                                               \
    if (checkError(ret, SQL_HANDLE_DBC, dbc, (SQLWCHAR*)L##ERROR)) { \
      exit(1);                                                       \
    }                                                                \
  } while (0)

int main(int argc, char** argv) {
  SQLHENV env;
  SQLHDBC dbc;
  SQLHSTMT stmt;
  SQLHSTMT stmt1;

  if (argc != 2) {
    std::cerr << "Expected exactly 1 argument: the DSN for connecting, got " << argc - 1
              << "arguments. exiting...";
    return 1;
  }

  std::string dsn(argv[1]);

  SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
  // we want ODBC3 support, set env handle
  SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, reinterpret_cast<void*>(SQL_OV_ODBC3_80), 0);
  // allocate connection handle
  SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

  // connect to the DSN using dbc handle
  CHECK_OK(SQLDriverConnectA(dbc, nullptr,
                             reinterpret_cast<SQLCHAR*>(const_cast<char*>(dsn.c_str())),
                             SQL_NTS, nullptr, 0, nullptr, SQL_DRIVER_COMPLETE),
           "Error -- Driver Connect failed");

  std::ofstream timing_output("odbc_perf_record");

  for (size_t iter = 0; iter < 100; iter++) {
    CHECK_OK(SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt),
             "Error -- Statement handle alloc failed");

    const auto start{std::chrono::steady_clock::now()};
    CHECK_OK(SQLExecDirect(
                 stmt,
                 (SQLCHAR*)"SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.LINEITEM "
                           "LIMIT 100000000",
                 SQL_NTS),
             "Error -- Statement execution failed");

    const SQLULEN bulkSize = 10000;
    CHECK_OK(SQLSetStmtAttr(stmt, SQL_ATTR_ROW_ARRAY_SIZE,
                            reinterpret_cast<SQLPOINTER>(bulkSize), 0),
             "Error -- SetStmtAttr failed");

    // bind columns to buffers
    SQLINTEGER val_orderkey[bulkSize];
    SQLINTEGER val_partkey[bulkSize];
    SQLINTEGER val_suppkey[bulkSize];
    SQLINTEGER val_linenumber[bulkSize];
    SQLDOUBLE val_quantity[bulkSize];
    SQLDOUBLE val_extendedprice[bulkSize];
    SQLDOUBLE val_discount[bulkSize];
    SQLDOUBLE val_tax[bulkSize];
    SQLCHAR val_retflag[bulkSize][2];
    SQLCHAR val_linestatus[bulkSize][2];
    SQL_DATE_STRUCT val_shipdate[bulkSize];
    SQL_DATE_STRUCT val_commitdate[bulkSize];
    SQL_DATE_STRUCT val_receiptdate[bulkSize];
    SQLCHAR val_shipinstruct[bulkSize][26];
    SQLCHAR val_shipmode[bulkSize][11];
    SQLCHAR val_comment[bulkSize][45];

    CHECK_OK(SQLBindCol(stmt, 1, SQL_C_LONG, reinterpret_cast<SQLPOINTER>(val_orderkey),
                        sizeof(val_orderkey), nullptr),
             "BindCol failed");
    CHECK_OK(SQLBindCol(stmt, 2, SQL_C_LONG, reinterpret_cast<SQLPOINTER>(val_partkey),
                        sizeof(val_partkey), nullptr),
             "BindCol failed");
    CHECK_OK(SQLBindCol(stmt, 3, SQL_C_LONG, reinterpret_cast<SQLPOINTER>(val_suppkey),
                        sizeof(val_suppkey), nullptr),
             "BindCol failed");
    CHECK_OK(SQLBindCol(stmt, 4, SQL_C_LONG, reinterpret_cast<SQLPOINTER>(val_linenumber),
                        sizeof(val_linenumber), nullptr),
             "BindCol failed");
    CHECK_OK(SQLBindCol(stmt, 5, SQL_C_DOUBLE, reinterpret_cast<SQLPOINTER>(val_quantity),
                        sizeof(val_quantity), nullptr),
             "BindCol failed");
    CHECK_OK(
        SQLBindCol(stmt, 6, SQL_C_DOUBLE, reinterpret_cast<SQLPOINTER>(val_extendedprice),
                   sizeof(val_extendedprice), nullptr),
        "BindCol failed");
    CHECK_OK(SQLBindCol(stmt, 7, SQL_C_DOUBLE, reinterpret_cast<SQLPOINTER>(val_discount),
                        sizeof(val_discount), nullptr),
             "BindCol failed");
    CHECK_OK(
        SQLBindCol(stmt, 8, SQL_C_DOUBLE, (SQLPOINTER)val_tax, sizeof(val_tax), nullptr),
        "BindCol failed");
    CHECK_OK(SQLBindCol(stmt, 9, SQL_C_CHAR, (SQLPOINTER)val_retflag,
                        sizeof(val_retflag[0]), nullptr),
             "BindCol failed");
    CHECK_OK(SQLBindCol(stmt, 10, SQL_C_CHAR, (SQLPOINTER)val_linestatus,
                        sizeof(val_linestatus[0]), nullptr),
             "BindCol failed");
    CHECK_OK(SQLBindCol(stmt, 11, SQL_C_DATE, (SQLPOINTER)val_shipdate,
                        sizeof(val_shipdate), nullptr),
             "BindCol failed");
    CHECK_OK(SQLBindCol(stmt, 12, SQL_C_DATE, (SQLPOINTER)val_commitdate,
                        sizeof(val_commitdate), nullptr),
             "BindCol failed");
    CHECK_OK(SQLBindCol(stmt, 13, SQL_C_DATE, (SQLPOINTER)val_receiptdate,
                        sizeof(val_receiptdate), nullptr),
             "BindCol failed");
    CHECK_OK(SQLBindCol(stmt, 14, SQL_C_CHAR, (SQLPOINTER)val_shipinstruct,
                        sizeof(val_shipinstruct[0]), nullptr),
             "BindCol failed");
    CHECK_OK(SQLBindCol(stmt, 15, SQL_C_CHAR, (SQLPOINTER)val_shipmode,
                        sizeof(val_shipmode[0]), nullptr),
             "BindCol failed");
    CHECK_OK(SQLBindCol(stmt, 16, SQL_C_CHAR, (SQLPOINTER)val_comment,
                        sizeof(val_comment[0]), nullptr),
             "BindCol failed");

    SQLRETURN ret;
    while (true) {
      ret = SQLFetch(stmt);
      if (checkError(ret, SQL_HANDLE_DBC, dbc, (SQLWCHAR*)L"fetch failed")) {
        exit(1);
      }
      if (ret == SQL_NO_DATA) break;
    }

    const auto end{std::chrono::steady_clock::now()};
    const std::chrono::duration<double> elapsed{end - start};
    timing_output << elapsed.count() << std::endl;

    SQLFreeStmt(stmt, SQL_CLOSE);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    if (iter % 10 == 0) {
      std::cout << "Run " << iter << std::endl;
      std::cout << "\tRuntime: " << elapsed.count() << " s" << std::endl;
    }
  }

  SQLDisconnect(dbc);
  SQLFreeHandle(SQL_HANDLE_DBC, dbc);
  SQLFreeHandle(SQL_HANDLE_ENV, env);
}
