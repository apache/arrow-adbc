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

WITH db_schemas AS (
    SELECT
        "database_name" as "catalog_name",
        "name" as "schema_name"
    FROM table(RESULT_SCAN(:SHOW_SCHEMA_QUERY_ID))
    WHERE "database_name" ILIKE :CATALOG
), db_info AS (
    SELECT "name" AS "database_name"
    FROM table(RESULT_SCAN(:SHOW_DB_QUERY_ID))
    WHERE "name" ILIKE :CATALOG
)
SELECT
    {
        'catalog_name': "database_name",
        'catalog_db_schemas': ARRAY_AGG(NULLIF({
            'db_schema_name': "schema_name",
            'db_schema_tables': null
        }, {}))
    } get_objects
FROM
    db_info
LEFT JOIN db_schemas
ON "database_name" = "catalog_name"
WHERE "database_name" ILIKE :CATALOG
GROUP BY "database_name";
