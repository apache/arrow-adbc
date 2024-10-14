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

WITH pk_constraints AS (
    SELECT
        "database_name" "table_catalog",
        "schema_name" "table_schema",
        "table_name" "table_name",
        "constraint_name" "constraint_name",
        'PRIMARY KEY' "constraint_type",
        ARRAY_AGG("column_name") WITHIN GROUP (ORDER BY "key_sequence") constraint_column_names,
        [] constraint_column_usage,
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID(-4)))
    WHERE "table_catalog" ILIKE :CATALOG AND "table_schema" ILIKE :DB_SCHEMA AND "table_name" ILIKE :TABLE
    GROUP BY "table_catalog", "table_schema", "table_name", "constraint_name"
),
unique_constraints AS (
    SELECT
        "database_name" "table_catalog",
        "schema_name" "table_schema",
        "table_name" "table_name",
        "constraint_name" "constraint_name",
        'UNIQUE' "constraint_type",
        ARRAY_AGG("column_name") WITHIN GROUP (ORDER BY "key_sequence") constraint_column_names,
        [] constraint_column_usage,
    FROM TABLE(RESULT_SCAN(:UNIQUE_QUERY_ID))
    WHERE "table_catalog" ILIKE :CATALOG AND "table_schema" ILIKE :DB_SCHEMA AND "table_name" ILIKE :TABLE
    GROUP BY "table_catalog", "table_schema", "table_name", "constraint_name"
),
fk_constraints AS (
    SELECT
        "fk_database_name" "table_catalog",
        "fk_schema_name" "table_schema",
        "fk_table_name" "table_name",
        "fk_name" "constraint_name",
        'FOREIGN KEY' "constraint_type",
        ARRAY_AGG("fk_column_name") WITHIN GROUP (ORDER BY "key_sequence") constraint_column_names,
        ARRAY_AGG({
            'fk_catalog': "pk_database_name",
            'fk_db_schema': "pk_schema_name",
            'fk_table': "pk_table_name",
            'fk_column_name': "pk_column_name"
        }) WITHIN GROUP (ORDER BY "key_sequence") constraint_column_usage,
    FROM TABLE(RESULT_SCAN(:FK_QUERY_ID))
    WHERE "table_catalog" ILIKE :CATALOG AND "table_schema" ILIKE :DB_SCHEMA AND "table_name" ILIKE :TABLE
    GROUP BY "table_catalog", "table_schema", "table_name", "constraint_name"
),
constraints AS (
    SELECT
        "table_catalog",
        "table_schema",
        "table_name",
        ARRAY_AGG({
            'constraint_name': "constraint_name",
            'constraint_type': "constraint_type",
            'constraint_column_names': constraint_column_names,
            'constraint_column_usage': constraint_column_usage
        }) table_constraints,
    FROM (
        SELECT * FROM pk_constraints
        UNION ALL
        SELECT * FROM unique_constraints
        UNION ALL
        SELECT * FROM fk_constraints
    )
    GROUP BY "table_catalog", "table_schema", "table_name"
),
tables AS (
SELECT
    "database_name" "catalog_name",
    "schema_name" "schema_name",
    ARRAY_AGG({
        'table_name': "name",
        'table_type': "kind",
        'table_constraints': table_constraints,
        'table_columns': null
    }) db_schema_tables
FROM TABLE(RESULT_SCAN(:SHOW_TABLE_QUERY_ID))
LEFT JOIN constraints
ON "database_name" = "table_catalog" AND "schema_name" = "table_schema" AND "name" = "table_name"
WHERE "database_name" ILIKE :CATALOG AND "schema_name" ILIKE :DB_SCHEMA AND "name" ILIKE :TABLE
GROUP BY "database_name", "schema_name"
),
db_schemas AS (
    SELECT
        "database_name" "catalog_name",
        "name" "schema_name",
        db_schema_tables,
    FROM TABLE(RESULT_SCAN(:SHOW_SCHEMA_QUERY_ID))
    LEFT JOIN tables
    ON "database_name" = "catalog_name" AND "name" = tables."schema_name"
    WHERE "database_name" ILIKE :CATALOG AND "name" ILIKE :DB_SCHEMA
)
SELECT
    {
        'catalog_name': "name",
        'catalog_db_schemas': ARRAY_AGG({
            'db_schema_name': db_schemas."schema_name",
            'db_schema_tables': db_schema_tables
        })
    } get_objects
FROM
    TABLE(RESULT_SCAN(:SHOW_DB_QUERY_ID))
LEFT JOIN db_schemas
ON "name" = "catalog_name"
WHERE "name" ILIKE :CATALOG
GROUP BY "name";
