// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http:--www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

WITH db_schemas AS (
    SELECT
        catalog_name,
        schema_name,
    FROM information_schema.schemata
    WHERE catalog_name ILIKE :CATALOG AND schema_name ILIKE :DB_SCHEMA
)
SELECT
    {
        'catalog_name': database_name,
        'catalog_db_schemas': ARRAY_AGG({
            'db_schema_name': schema_name,
            'db_schema_tables': null
        })
    } get_objects
FROM
    information_schema.databases
LEFT JOIN db_schemas
ON database_name = catalog_name
WHERE database_name ILIKE :CATALOG
GROUP BY database_name;
