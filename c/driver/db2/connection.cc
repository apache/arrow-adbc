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

#include "connection.h"

#include <algorithm>
#include <cstring>
#include <sstream>

#include <nanoarrow/nanoarrow.hpp>

#include "error.h"
#include "type_mapping.h"

namespace adbc::db2 {

// ---------------------------------------------------------------------------
// Db2Connection
// ---------------------------------------------------------------------------

Status Db2Connection::InitImpl(void* parent) {
  database_ = reinterpret_cast<Db2Database*>(parent);

  UNWRAP_RESULT(hdbc_, database_->AllocConnection());

  const std::string& conn_str = database_->connection_string();
  SQLCHAR out_str[1024];
  SQLSMALLINT out_len = 0;
  SQLRETURN rc = SQLDriverConnect(
      hdbc_, nullptr, const_cast<SQLCHAR*>(reinterpret_cast<const SQLCHAR*>(conn_str.c_str())),
      static_cast<SQLSMALLINT>(conn_str.size()), out_str, sizeof(out_str), &out_len,
      SQL_DRIVER_NOPROMPT);
  UNWRAP_STATUS(CheckRc(SQL_HANDLE_DBC, hdbc_, rc, "SQLDriverConnect"));

  rc = SQLSetConnectAttr(hdbc_, SQL_ATTR_AUTOCOMMIT,
                         reinterpret_cast<SQLPOINTER>(SQL_AUTOCOMMIT_ON), 0);
  UNWRAP_STATUS(CheckRc(SQL_HANDLE_DBC, hdbc_, rc, "SQLSetConnectAttr(AUTOCOMMIT)"));

  return status::Ok();
}

Status Db2Connection::ReleaseImpl() {
  if (hdbc_ != SQL_NULL_HDBC) {
    SQLDisconnect(hdbc_);
    database_->FreeConnection(hdbc_);
    hdbc_ = SQL_NULL_HDBC;
  }
  return status::Ok();
}

Status Db2Connection::CommitImpl() {
  SQLRETURN rc = SQLEndTran(SQL_HANDLE_DBC, hdbc_, SQL_COMMIT);
  UNWRAP_STATUS(CheckRc(SQL_HANDLE_DBC, hdbc_, rc, "SQLEndTran(COMMIT)"));
  return status::Ok();
}

Status Db2Connection::RollbackImpl() {
  SQLRETURN rc = SQLEndTran(SQL_HANDLE_DBC, hdbc_, SQL_ROLLBACK);
  UNWRAP_STATUS(CheckRc(SQL_HANDLE_DBC, hdbc_, rc, "SQLEndTran(ROLLBACK)"));
  return status::Ok();
}

Status Db2Connection::ToggleAutocommitImpl(bool enable_autocommit) {
  SQLPOINTER val = reinterpret_cast<SQLPOINTER>(
      enable_autocommit ? SQL_AUTOCOMMIT_ON : SQL_AUTOCOMMIT_OFF);
  SQLRETURN rc = SQLSetConnectAttr(hdbc_, SQL_ATTR_AUTOCOMMIT, val, 0);
  UNWRAP_STATUS(CheckRc(SQL_HANDLE_DBC, hdbc_, rc, "SQLSetConnectAttr(AUTOCOMMIT)"));
  return status::Ok();
}

Result<std::vector<InfoValue>> Db2Connection::InfoImpl(
    const std::vector<uint32_t>& codes) {
  std::vector<InfoValue> infos;

  SQLCHAR info_buf[256];
  SQLSMALLINT info_len = 0;

  for (uint32_t code : codes) {
    switch (code) {
      case ADBC_INFO_VENDOR_NAME: {
        SQLRETURN rc = SQLGetInfo(hdbc_, SQL_DBMS_NAME, info_buf, sizeof(info_buf),
                                 &info_len);
        if (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO) {
          infos.push_back(
              {code, std::string(reinterpret_cast<char*>(info_buf), info_len)});
        } else {
          infos.push_back({code, std::string("IBM DB2")});
        }
        break;
      }
      case ADBC_INFO_VENDOR_VERSION: {
        SQLRETURN rc = SQLGetInfo(hdbc_, SQL_DBMS_VER, info_buf, sizeof(info_buf),
                                 &info_len);
        if (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO) {
          infos.push_back(
              {code, std::string(reinterpret_cast<char*>(info_buf), info_len)});
        }
        break;
      }
      case ADBC_INFO_DRIVER_NAME:
        infos.push_back({code, std::string("ADBC DB2 Driver")});
        break;
      case ADBC_INFO_DRIVER_VERSION:
        infos.push_back({code, std::string("1.0.0")});
        break;
      case ADBC_INFO_DRIVER_ADBC_VERSION:
        infos.push_back({code, static_cast<int64_t>(ADBC_VERSION_1_1_0)});
        break;
      default:
        break;
    }
  }
  return infos;
}

Result<std::unique_ptr<GetObjectsHelper>> Db2Connection::GetObjectsImpl() {
  return std::make_unique<Db2GetObjectsHelper>(hdbc_);
}

Status Db2Connection::GetTableSchemaImpl(std::optional<std::string_view> catalog,
                                         std::optional<std::string_view> db_schema,
                                         std::string_view table_name,
                                         ArrowSchema* schema) {
  SQLHSTMT hstmt = SQL_NULL_HSTMT;
  SQLRETURN rc = SQLAllocHandle(SQL_HANDLE_STMT, hdbc_, &hstmt);
  UNWRAP_STATUS(CheckRc(SQL_HANDLE_DBC, hdbc_, rc, "SQLAllocHandle(STMT)"));

  // Build a query that returns zero rows but exposes the column metadata.
  std::ostringstream oss;
  oss << "SELECT * FROM ";
  if (db_schema.has_value() && !db_schema->empty()) {
    oss << "\"" << *db_schema << "\".";
  }
  oss << "\"" << table_name << "\" WHERE 1=0";

  std::string query = oss.str();
  rc = SQLExecDirect(hstmt,
                     const_cast<SQLCHAR*>(reinterpret_cast<const SQLCHAR*>(query.c_str())),
                     static_cast<SQLINTEGER>(query.size()));
  if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA) {
    Status err = Db2Error(SQL_HANDLE_STMT, hstmt, "SQLExecDirect(GetTableSchema)");
    SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    return err;
  }

  SQLSMALLINT num_cols = 0;
  rc = SQLNumResultCols(hstmt, &num_cols);
  if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
    Status err = Db2Error(SQL_HANDLE_STMT, hstmt, "SQLNumResultCols");
    SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    return err;
  }

  ArrowSchemaInit(schema);
  UNWRAP_ERRNO(Internal, ArrowSchemaSetTypeStruct(schema, num_cols));

  for (SQLSMALLINT i = 0; i < num_cols; i++) {
    SQLCHAR col_name[256];
    SQLSMALLINT name_len = 0;
    SQLSMALLINT sql_type = 0;
    SQLULEN col_size = 0;
    SQLSMALLINT decimal_digits = 0;
    SQLSMALLINT nullable = SQL_NULLABLE_UNKNOWN;

    rc = SQLDescribeCol(hstmt, static_cast<SQLUSMALLINT>(i + 1), col_name,
                        sizeof(col_name), &name_len, &sql_type, &col_size,
                        &decimal_digits, &nullable);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
      Status err = Db2Error(SQL_HANDLE_STMT, hstmt, "SQLDescribeCol");
      schema->release(schema);
      SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
      return err;
    }

    Db2Column db2_col;
    UNWRAP_STATUS(
        MapDb2TypeToArrow(sql_type, col_size, decimal_digits, nullable, &db2_col));

    name_len = std::min(name_len, static_cast<SQLSMALLINT>(sizeof(col_name) - 1));
    col_name[name_len] = '\0';
    Status s = SetSchemaFromDb2Column(schema->children[i],
                                      reinterpret_cast<const char*>(col_name), db2_col);
    if (!s.ok()) {
      schema->release(schema);
      SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
      return s;
    }
  }

  SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
  return status::Ok();
}

Result<std::vector<std::string>> Db2Connection::GetTableTypesImpl() {
  return std::vector<std::string>{"TABLE", "VIEW", "ALIAS", "SYNONYM",
                                  "SYSTEM TABLE"};
}

Result<std::optional<std::string>> Db2Connection::GetCurrentCatalogImpl() {
  SQLCHAR buf[256];
  SQLINTEGER len = 0;
  SQLRETURN rc =
      SQLGetConnectAttr(hdbc_, SQL_ATTR_CURRENT_CATALOG, buf, sizeof(buf), &len);
  if (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO) {
    if (len > static_cast<SQLINTEGER>(sizeof(buf) - 1)) {
      len = static_cast<SQLINTEGER>(sizeof(buf) - 1);
    }
    return std::string(reinterpret_cast<char*>(buf), len);
  }
  return std::nullopt;
}

Result<std::optional<std::string>> Db2Connection::GetCurrentSchemaImpl() {
  SQLHSTMT hstmt = SQL_NULL_HSTMT;
  SQLRETURN rc = SQLAllocHandle(SQL_HANDLE_STMT, hdbc_, &hstmt);
  if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
    return std::nullopt;
  }

  rc = SQLExecDirect(
      hstmt,
      const_cast<SQLCHAR*>(
          reinterpret_cast<const SQLCHAR*>("VALUES CURRENT SCHEMA")),
      SQL_NTS);
  if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
    SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    return std::nullopt;
  }

  SQLCHAR buf[128];
  SQLLEN ind = 0;
  SQLBindCol(hstmt, 1, SQL_C_CHAR, buf, sizeof(buf), &ind);

  std::optional<std::string> result = std::nullopt;
  if (SQLFetch(hstmt) == SQL_SUCCESS && ind != SQL_NULL_DATA && ind > 0) {
    size_t len = std::min(static_cast<size_t>(ind), sizeof(buf) - 1);
    std::string schema(reinterpret_cast<char*>(buf), len);
    while (!schema.empty() && schema.back() == ' ') schema.pop_back();
    if (!schema.empty()) {
      result = std::move(schema);
    }
  }

  SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
  return result;
}

AdbcStatusCode Db2Connection::Cancel(AdbcError* error) {
  SQLHSTMT h = active_hstmt_.load();
  if (h != SQL_NULL_HSTMT) {
    SQLRETURN rc = SQLCancel(h);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
      return Db2Error(SQL_HANDLE_STMT, h, "SQLCancel").ToAdbc(error);
    }
  }
  return ADBC_STATUS_OK;
}

// ---------------------------------------------------------------------------
// Db2GetObjectsHelper
// ---------------------------------------------------------------------------

Db2GetObjectsHelper::Db2GetObjectsHelper(SQLHDBC hdbc) : hdbc_(hdbc) {}

Status Db2GetObjectsHelper::Load(
    driver::GetObjectsDepth depth,
    std::optional<std::string_view> catalog_filter,
    std::optional<std::string_view> schema_filter,
    std::optional<std::string_view> table_filter,
    std::optional<std::string_view> column_filter,
    const std::vector<std::string_view>& table_types) {
  return status::Ok();
}

Status Db2GetObjectsHelper::LoadCatalogs(
    std::optional<std::string_view> catalog_filter) {
  catalogs_.clear();
  next_catalog_ = 0;

  SQLHSTMT hstmt = SQL_NULL_HSTMT;
  SQLRETURN rc = SQLAllocHandle(SQL_HANDLE_STMT, hdbc_, &hstmt);
  UNWRAP_STATUS(CheckRc(SQL_HANDLE_DBC, hdbc_, rc, "SQLAllocHandle(catalog)"));

  // SQL_ALL_CATALOGS: returns catalog list
  rc = SQLTables(hstmt, reinterpret_cast<SQLCHAR*>(const_cast<char*>("%")), SQL_NTS,
                 reinterpret_cast<SQLCHAR*>(const_cast<char*>("")), 0,
                 reinterpret_cast<SQLCHAR*>(const_cast<char*>("")), 0,
                 reinterpret_cast<SQLCHAR*>(const_cast<char*>("")), 0);
  if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
    Status err = Db2Error(SQL_HANDLE_STMT, hstmt, "SQLTables(catalogs)");
    SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    return err;
  }

  SQLCHAR cat_buf[256];
  SQLLEN cat_ind = 0;
  SQLBindCol(hstmt, 1, SQL_C_CHAR, cat_buf, sizeof(cat_buf), &cat_ind);

  while (SQLFetch(hstmt) == SQL_SUCCESS) {
    if (cat_ind != SQL_NULL_DATA && cat_ind > 0) {
      size_t len = std::min(static_cast<size_t>(cat_ind), sizeof(cat_buf) - 1);
      std::string cat(reinterpret_cast<char*>(cat_buf), len);
      if (!catalog_filter.has_value() || cat == *catalog_filter) {
        catalogs_.push_back(std::move(cat));
      }
    }
  }

  SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

  // If no filter was specified and DB2 didn't enumerate catalogs,
  // add an empty string so the framework iterates at least once.
  // But if a filter was specified and nothing matched, return empty
  // so nonexistent catalogs correctly yield zero results.
  if (catalogs_.empty() && !catalog_filter.has_value()) {
    catalogs_.push_back("");
  }
  return status::Ok();
}

Result<std::optional<std::string_view>> Db2GetObjectsHelper::NextCatalog() {
  if (next_catalog_ >= catalogs_.size()) return std::nullopt;
  current_catalog_ = catalogs_[next_catalog_++];
  return std::string_view(current_catalog_);
}

Status Db2GetObjectsHelper::LoadSchemas(
    std::string_view catalog, std::optional<std::string_view> schema_filter) {
  schemas_.clear();
  next_schema_ = 0;

  SQLHSTMT hstmt = SQL_NULL_HSTMT;
  SQLRETURN rc = SQLAllocHandle(SQL_HANDLE_STMT, hdbc_, &hstmt);
  UNWRAP_STATUS(CheckRc(SQL_HANDLE_DBC, hdbc_, rc, "SQLAllocHandle(schemas)"));

  rc = SQLTables(
      hstmt,
      const_cast<SQLCHAR*>(reinterpret_cast<const SQLCHAR*>(catalog.data())),
      static_cast<SQLSMALLINT>(catalog.size()),
      reinterpret_cast<SQLCHAR*>(const_cast<char*>("%")), SQL_NTS,
      reinterpret_cast<SQLCHAR*>(const_cast<char*>("")), 0,
      reinterpret_cast<SQLCHAR*>(const_cast<char*>("")), 0);
  if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
    Status err = Db2Error(SQL_HANDLE_STMT, hstmt, "SQLTables(schemas)");
    SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    return err;
  }

  SQLCHAR schema_buf[256];
  SQLLEN schema_ind = 0;
  SQLBindCol(hstmt, 2, SQL_C_CHAR, schema_buf, sizeof(schema_buf), &schema_ind);

  while (SQLFetch(hstmt) == SQL_SUCCESS) {
    if (schema_ind != SQL_NULL_DATA && schema_ind > 0) {
      size_t len = std::min(static_cast<size_t>(schema_ind), sizeof(schema_buf) - 1);
      std::string s(reinterpret_cast<char*>(schema_buf), len);
      if (!schema_filter.has_value() || s == *schema_filter) {
        // Deduplicate
        bool found = false;
        for (const auto& existing : schemas_) {
          if (existing == s) {
            found = true;
            break;
          }
        }
        if (!found) schemas_.push_back(std::move(s));
      }
    }
  }

  SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
  return status::Ok();
}

Result<std::optional<std::string_view>> Db2GetObjectsHelper::NextSchema() {
  if (next_schema_ >= schemas_.size()) return std::nullopt;
  current_schema_ = schemas_[next_schema_++];
  return std::string_view(current_schema_);
}

Status Db2GetObjectsHelper::LoadTables(
    std::string_view catalog, std::string_view schema,
    std::optional<std::string_view> table_filter,
    const std::vector<std::string_view>& table_types) {
  tables_.clear();
  next_table_ = 0;

  SQLHSTMT hstmt = SQL_NULL_HSTMT;
  SQLRETURN rc = SQLAllocHandle(SQL_HANDLE_STMT, hdbc_, &hstmt);
  UNWRAP_STATUS(CheckRc(SQL_HANDLE_DBC, hdbc_, rc, "SQLAllocHandle(tables)"));

  std::string type_list;
  for (size_t i = 0; i < table_types.size(); i++) {
    if (i > 0) type_list += ",";
    type_list += table_types[i];
  }

  const char* tbl_filter =
      table_filter.has_value() ? table_filter->data() : "%";
  SQLSMALLINT tbl_filter_len =
      table_filter.has_value() ? static_cast<SQLSMALLINT>(table_filter->size()) : SQL_NTS;

  rc = SQLTables(
      hstmt,
      const_cast<SQLCHAR*>(reinterpret_cast<const SQLCHAR*>(catalog.data())),
      static_cast<SQLSMALLINT>(catalog.size()),
      const_cast<SQLCHAR*>(reinterpret_cast<const SQLCHAR*>(schema.data())),
      static_cast<SQLSMALLINT>(schema.size()),
      const_cast<SQLCHAR*>(reinterpret_cast<const SQLCHAR*>(tbl_filter)),
      tbl_filter_len,
      type_list.empty()
          ? nullptr
          : const_cast<SQLCHAR*>(
                reinterpret_cast<const SQLCHAR*>(type_list.c_str())),
      type_list.empty() ? 0 : static_cast<SQLSMALLINT>(type_list.size()));
  if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
    Status err = Db2Error(SQL_HANDLE_STMT, hstmt, "SQLTables");
    SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    return err;
  }

  SQLCHAR name_buf[256];
  SQLLEN name_ind = 0;
  SQLCHAR type_buf[256];
  SQLLEN type_ind = 0;
  SQLBindCol(hstmt, 3, SQL_C_CHAR, name_buf, sizeof(name_buf), &name_ind);
  SQLBindCol(hstmt, 4, SQL_C_CHAR, type_buf, sizeof(type_buf), &type_ind);

  while (SQLFetch(hstmt) == SQL_SUCCESS) {
    std::string name =
        (name_ind != SQL_NULL_DATA && name_ind > 0)
            ? std::string(reinterpret_cast<char*>(name_buf),
                          std::min(static_cast<size_t>(name_ind), sizeof(name_buf) - 1))
            : "";
    std::string type =
        (type_ind != SQL_NULL_DATA && type_ind > 0)
            ? std::string(reinterpret_cast<char*>(type_buf),
                          std::min(static_cast<size_t>(type_ind), sizeof(type_buf) - 1))
            : "TABLE";
    tables_.emplace_back(std::move(name), std::move(type));
  }

  SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
  return status::Ok();
}

Result<std::optional<GetObjectsHelper::Table>> Db2GetObjectsHelper::NextTable() {
  if (next_table_ >= tables_.size()) return std::nullopt;
  const auto& t = tables_[next_table_++];
  return Table{t.first, t.second};
}

Status Db2GetObjectsHelper::LoadColumns(
    std::string_view catalog, std::string_view schema, std::string_view table,
    std::optional<std::string_view> column_filter) {
  columns_.clear();
  column_names_.clear();
  next_column_ = 0;

  SQLHSTMT hstmt = SQL_NULL_HSTMT;
  SQLRETURN rc = SQLAllocHandle(SQL_HANDLE_STMT, hdbc_, &hstmt);
  UNWRAP_STATUS(CheckRc(SQL_HANDLE_DBC, hdbc_, rc, "SQLAllocHandle(columns)"));

  const char* col_filter =
      column_filter.has_value() ? column_filter->data() : "%";
  SQLSMALLINT col_filter_len =
      column_filter.has_value()
          ? static_cast<SQLSMALLINT>(column_filter->size())
          : SQL_NTS;

  rc = SQLColumns(
      hstmt,
      const_cast<SQLCHAR*>(reinterpret_cast<const SQLCHAR*>(catalog.data())),
      static_cast<SQLSMALLINT>(catalog.size()),
      const_cast<SQLCHAR*>(reinterpret_cast<const SQLCHAR*>(schema.data())),
      static_cast<SQLSMALLINT>(schema.size()),
      const_cast<SQLCHAR*>(reinterpret_cast<const SQLCHAR*>(table.data())),
      static_cast<SQLSMALLINT>(table.size()),
      const_cast<SQLCHAR*>(reinterpret_cast<const SQLCHAR*>(col_filter)),
      col_filter_len);
  if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
    Status err = Db2Error(SQL_HANDLE_STMT, hstmt, "SQLColumns");
    SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    return err;
  }

  // Result set columns: 4=COLUMN_NAME, 5=DATA_TYPE, 11=NULLABLE, 17=ORDINAL_POSITION
  SQLCHAR col_name[256];
  SQLLEN col_name_ind = 0;
  SQLSMALLINT data_type = 0;
  SQLLEN data_type_ind = 0;
  SQLSMALLINT nullable_val = 0;
  SQLLEN nullable_ind = 0;
  SQLSMALLINT ordinal = 0;
  SQLLEN ordinal_ind = 0;

  SQLBindCol(hstmt, 4, SQL_C_CHAR, col_name, sizeof(col_name), &col_name_ind);
  SQLBindCol(hstmt, 5, SQL_C_SSHORT, &data_type, sizeof(data_type), &data_type_ind);
  SQLBindCol(hstmt, 11, SQL_C_SSHORT, &nullable_val, sizeof(nullable_val),
             &nullable_ind);
  SQLBindCol(hstmt, 17, SQL_C_SSHORT, &ordinal, sizeof(ordinal), &ordinal_ind);

  while (SQLFetch(hstmt) == SQL_SUCCESS) {
    std::string name_str =
        (col_name_ind != SQL_NULL_DATA && col_name_ind > 0)
            ? std::string(reinterpret_cast<char*>(col_name),
                          std::min(static_cast<size_t>(col_name_ind),
                                   sizeof(col_name) - 1))
            : "";
    column_names_.push_back(std::move(name_str));

    Column c;
    c.column_name = column_names_.back();
    c.ordinal_position = (ordinal_ind != SQL_NULL_DATA) ? ordinal : 0;

    GetObjectsHelper::ColumnXdbc xdbc;
    xdbc.xdbc_nullable = (nullable_ind != SQL_NULL_DATA)
                              ? std::make_optional(static_cast<int16_t>(nullable_val))
                              : std::nullopt;
    xdbc.xdbc_sql_data_type =
        (data_type_ind != SQL_NULL_DATA)
            ? std::make_optional(static_cast<int16_t>(data_type))
            : std::nullopt;
    c.xdbc = xdbc;
    columns_.push_back(std::move(c));
  }

  SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

  UNWRAP_STATUS(LoadConstraintsFor(catalog, schema, table));
  return status::Ok();
}

Result<std::optional<GetObjectsHelper::Column>>
Db2GetObjectsHelper::NextColumn() {
  if (next_column_ >= columns_.size()) return std::nullopt;
  return columns_[next_column_++];
}

Status Db2GetObjectsHelper::LoadConstraintsFor(
    std::string_view catalog, std::string_view schema, std::string_view table) {
  constraints_.clear();
  next_constraint_ = 0;

  // --- Primary keys ---
  SQLHSTMT hstmt = SQL_NULL_HSTMT;
  SQLRETURN rc = SQLAllocHandle(SQL_HANDLE_STMT, hdbc_, &hstmt);
  UNWRAP_STATUS(CheckRc(SQL_HANDLE_DBC, hdbc_, rc, "SQLAllocHandle(pk)"));

  rc = SQLPrimaryKeys(
      hstmt,
      catalog.empty() ? nullptr
                      : const_cast<SQLCHAR*>(
                            reinterpret_cast<const SQLCHAR*>(catalog.data())),
      catalog.empty() ? 0 : static_cast<SQLSMALLINT>(catalog.size()),
      const_cast<SQLCHAR*>(reinterpret_cast<const SQLCHAR*>(schema.data())),
      static_cast<SQLSMALLINT>(schema.size()),
      const_cast<SQLCHAR*>(reinterpret_cast<const SQLCHAR*>(table.data())),
      static_cast<SQLSMALLINT>(table.size()));

  if (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO) {
    SQLCHAR pk_name[256];
    SQLLEN pk_name_ind = 0;
    SQLCHAR col_name[256];
    SQLLEN col_name_ind = 0;
    SQLBindCol(hstmt, 4, SQL_C_CHAR, col_name, sizeof(col_name), &col_name_ind);
    SQLBindCol(hstmt, 6, SQL_C_CHAR, pk_name, sizeof(pk_name), &pk_name_ind);

    std::string current_pk_name;
    Constraint current_pk;
    current_pk.type = "PRIMARY KEY";
    bool have_pk = false;

    while (SQLFetch(hstmt) == SQL_SUCCESS) {
      std::string cn = (pk_name_ind > 0 && pk_name_ind != SQL_NULL_DATA)
                            ? std::string(reinterpret_cast<char*>(pk_name),
                                          std::min(static_cast<size_t>(pk_name_ind),
                                                   sizeof(pk_name) - 1))
                            : "";
      if (!have_pk || cn != current_pk_name) {
        if (have_pk) {
          constraints_.push_back(current_pk);
        }
        current_pk = Constraint{};
        current_pk.type = "PRIMARY KEY";
        constraint_names_.push_back(cn);
        current_pk.name = std::string_view(constraint_names_.back());
        current_pk_name = cn;
        have_pk = true;
      }
      if (col_name_ind > 0 && col_name_ind != SQL_NULL_DATA) {
        constraint_col_names_.emplace_back(
            reinterpret_cast<char*>(col_name),
            std::min(static_cast<size_t>(col_name_ind), sizeof(col_name) - 1));
        current_pk.column_names.push_back(constraint_col_names_.back());
      }
    }
    if (have_pk) {
      constraints_.push_back(current_pk);
    }
  }
  SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

  // --- Foreign keys ---
  hstmt = SQL_NULL_HSTMT;
  rc = SQLAllocHandle(SQL_HANDLE_STMT, hdbc_, &hstmt);
  UNWRAP_STATUS(CheckRc(SQL_HANDLE_DBC, hdbc_, rc, "SQLAllocHandle(fk)"));

  rc = SQLForeignKeys(
      hstmt,
      nullptr, 0, nullptr, 0, nullptr, 0,
      catalog.empty() ? nullptr
                      : const_cast<SQLCHAR*>(
                            reinterpret_cast<const SQLCHAR*>(catalog.data())),
      catalog.empty() ? 0 : static_cast<SQLSMALLINT>(catalog.size()),
      const_cast<SQLCHAR*>(reinterpret_cast<const SQLCHAR*>(schema.data())),
      static_cast<SQLSMALLINT>(schema.size()),
      const_cast<SQLCHAR*>(reinterpret_cast<const SQLCHAR*>(table.data())),
      static_cast<SQLSMALLINT>(table.size()));

  if (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO) {
    SQLCHAR fk_name[256];
    SQLLEN fk_name_ind = 0;
    SQLCHAR fk_col[256];
    SQLLEN fk_col_ind = 0;
    SQLBindCol(hstmt, 8, SQL_C_CHAR, fk_col, sizeof(fk_col), &fk_col_ind);
    SQLBindCol(hstmt, 12, SQL_C_CHAR, fk_name, sizeof(fk_name), &fk_name_ind);

    std::string current_fk_name;
    Constraint current_fk;
    current_fk.type = "FOREIGN KEY";
    bool have_fk = false;

    while (SQLFetch(hstmt) == SQL_SUCCESS) {
      std::string fn = (fk_name_ind > 0 && fk_name_ind != SQL_NULL_DATA)
                            ? std::string(reinterpret_cast<char*>(fk_name),
                                          std::min(static_cast<size_t>(fk_name_ind),
                                                   sizeof(fk_name) - 1))
                            : "";
      if (!have_fk || fn != current_fk_name) {
        if (have_fk) {
          constraints_.push_back(current_fk);
        }
        current_fk = Constraint{};
        current_fk.type = "FOREIGN KEY";
        constraint_names_.push_back(fn);
        current_fk.name = std::string_view(constraint_names_.back());
        current_fk_name = fn;
        have_fk = true;
      }
      if (fk_col_ind > 0 && fk_col_ind != SQL_NULL_DATA) {
        constraint_col_names_.emplace_back(
            reinterpret_cast<char*>(fk_col),
            std::min(static_cast<size_t>(fk_col_ind), sizeof(fk_col) - 1));
        current_fk.column_names.push_back(constraint_col_names_.back());
      }
    }
    if (have_fk) {
      constraints_.push_back(current_fk);
    }
  }
  SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

  return status::Ok();
}

Result<std::optional<GetObjectsHelper::Constraint>>
Db2GetObjectsHelper::NextConstraint() {
  if (next_constraint_ >= constraints_.size()) return std::nullopt;
  return constraints_[next_constraint_++];
}

Status Db2GetObjectsHelper::Close() { return status::Ok(); }

}  // namespace adbc::db2
