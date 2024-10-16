-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

WITH tables AS (
SELECT
    "database_name" "catalog_name",
    "schema_name" "schema_name",
    ARRAY_AGG({
        'table_name': "name",
        'table_type': "kind",
        'table_constraints': null,
        'table_columns': null
    }) db_schema_tables
FROM TABLE(RESULT_SCAN(:SHOW_TABLE_QUERY_ID))
WHERE "database_name" ILIKE :CATALOG AND "schema_name" ILIKE :DB_SCHEMA AND "name" ILIKE :TABLE
GROUP BY "database_name", "schema_name"
),
db_schemas AS (
    SELECT
        "database_name" "catalog_name",
        "name" "schema_name",
        COALESCE(db_schema_tables, []) db_schema_tables,
    FROM TABLE(RESULT_SCAN(:SHOW_SCHEMA_QUERY_ID))
    LEFT JOIN tables
    ON "database_name" = "catalog_name" AND "name" = tables."schema_name"
    WHERE "database_name" ILIKE :CATALOG AND "name" ILIKE :DB_SCHEMA
)
SELECT
    {
        'catalog_name': "name",
        'catalog_db_schemas': ARRAY_AGG(NULLIF({
            'db_schema_name': db_schemas."schema_name",
            'db_schema_tables': db_schema_tables
        }, {}))
    } get_objects
FROM
    TABLE(RESULT_SCAN(:SHOW_DB_QUERY_ID))
LEFT JOIN db_schemas
ON "name" = "catalog_name"
WHERE "name" ILIKE :CATALOG
GROUP BY "name";
