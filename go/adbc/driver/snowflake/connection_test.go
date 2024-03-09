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

package snowflake

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPrepareCatalogsSQL(t *testing.T) {
	expected := "SHOW TERSE DATABASES"
	actual := prepareCatalogsSQL()

	assert.Equal(t, expected, actual, "The expected SQL query for catalogs is not being generated")
}

func TestPrepareDbSchemasSQLWithNoFilterOneCatalog(t *testing.T) {
	catalogNames := [1]string{"DEMO_DB"}
	catalogPattern := ""
	schemaPattern := ""

	expected := `SELECT CATALOG_NAME, SCHEMA_NAME
					FROM
					(
						SELECT * FROM "DEMO_DB".INFORMATION_SCHEMA.SCHEMATA
					)`
	actual, queryArgs := prepareDbSchemasSQL(catalogNames[:], &catalogPattern, &schemaPattern)

	println("Query Args", queryArgs)
	assert.True(t, areStringsEquivalent(expected, actual), "The expected SQL query for DbSchemas is not being generated")
}

func TestPrepareDbSchemasSQLWithNoFilter(t *testing.T) {
	catalogNames := [3]string{"DEMO_DB", "DEMO'DB", "HELLO_DB"}
	catalogPattern := ""
	schemaPattern := ""

	expected := `SELECT CATALOG_NAME, SCHEMA_NAME
					FROM
					(
						SELECT * FROM "DEMO_DB".INFORMATION_SCHEMA.SCHEMATA
						UNION ALL
						SELECT * FROM "DEMO'DB".INFORMATION_SCHEMA.SCHEMATA
						UNION ALL
						SELECT * FROM "HELLO_DB".INFORMATION_SCHEMA.SCHEMATA
					)`
	actual, queryArgs := prepareDbSchemasSQL(catalogNames[:], &catalogPattern, &schemaPattern)

	println("Query Args", queryArgs)
	assert.True(t, areStringsEquivalent(expected, actual), "The expected SQL query for DbSchemas is not being generated")
}

func TestPrepareDbSchemasSQLWithCatalogFilter(t *testing.T) {
	catalogNames := [3]string{"DEMO_DB", "DEMO'DB", "HELLO_DB"}
	catalogPattern := "DEMO_DB"
	schemaPattern := ""

	expected := `SELECT CATALOG_NAME, SCHEMA_NAME
					FROM
					(
						SELECT * FROM "DEMO_DB".INFORMATION_SCHEMA.SCHEMATA
						UNION ALL
						SELECT * FROM "DEMO'DB".INFORMATION_SCHEMA.SCHEMATA
						UNION ALL
						SELECT * FROM "HELLO_DB".INFORMATION_SCHEMA.SCHEMATA
					)
					WHERE  CATALOG_NAME ILIKE ? `

	actual, queryArgs := prepareDbSchemasSQL(catalogNames[:], &catalogPattern, &schemaPattern)

	stringqueryArgs := make([]string, len(queryArgs)) // Pre-allocate the right size
	for index := range queryArgs {
		stringqueryArgs[index] = fmt.Sprintf("%v", queryArgs[index])
	}

	assert.True(t, areStringsEquivalent(catalogPattern, strings.Join(stringqueryArgs, ",")), "The expected CATALOG_NAME is not being generated")
	assert.True(t, areStringsEquivalent(expected, actual), "The expected SQL query for DbSchemas is not being generated")
}

func TestPrepareDbSchemasSQLWithSchemaFilter(t *testing.T) {
	catalogNames := [3]string{"DEMO_DB", "DEMO'DB", "HELLO_DB"}
	catalogPattern := ""
	schemaPattern := "PUBLIC"

	expected := `SELECT CATALOG_NAME, SCHEMA_NAME
					FROM
					(
						SELECT * FROM "DEMO_DB".INFORMATION_SCHEMA.SCHEMATA
						UNION ALL
						SELECT * FROM "DEMO'DB".INFORMATION_SCHEMA.SCHEMATA
						UNION ALL
						SELECT * FROM "HELLO_DB".INFORMATION_SCHEMA.SCHEMATA
					)
					WHERE  SCHEMA_NAME ILIKE ? `
	actual, queryArgs := prepareDbSchemasSQL(catalogNames[:], &catalogPattern, &schemaPattern)

	stringqueryArgs := make([]string, len(queryArgs)) // Pre-allocate the right size
	for index := range queryArgs {
		stringqueryArgs[index] = fmt.Sprintf("%v", queryArgs[index])
	}

	assert.True(t, areStringsEquivalent(schemaPattern, strings.Join(stringqueryArgs, ",")), "The expected SCHEMA_NAME is not being generated")
	assert.True(t, areStringsEquivalent(expected, actual), "The expected SQL query for DbSchemas is not being generated")
}

func TestPrepareDbSchemasSQL(t *testing.T) {
	catalogNames := [4]string{"DEMO_DB", "DEMOADB", "DEMO'DB", "HELLO_DB"}
	catalogPattern := "DEMO_DB"
	schemaPattern := "PUBLIC"

	expected := `SELECT CATALOG_NAME, SCHEMA_NAME
					FROM
					(
						SELECT * FROM "DEMO_DB".INFORMATION_SCHEMA.SCHEMATA
						UNION ALL
						SELECT * FROM "DEMOADB".INFORMATION_SCHEMA.SCHEMATA
						UNION ALL
						SELECT * FROM "DEMO'DB".INFORMATION_SCHEMA.SCHEMATA
						UNION ALL
						SELECT * FROM "HELLO_DB".INFORMATION_SCHEMA.SCHEMATA
					)
					WHERE  CATALOG_NAME ILIKE ? AND  SCHEMA_NAME ILIKE ? `
	actual, queryArgs := prepareDbSchemasSQL(catalogNames[:], &catalogPattern, &schemaPattern)

	stringqueryArgs := make([]string, len(queryArgs)) // Pre-allocate the right size
	for index := range queryArgs {
		stringqueryArgs[index] = fmt.Sprintf("%v", queryArgs[index])
	}

	assert.True(t, areStringsEquivalent(catalogPattern+","+schemaPattern, strings.Join(stringqueryArgs, ",")), "The expected SCHEMA_NAME is not being generated")

	assert.True(t, areStringsEquivalent(expected, actual), "The expected SQL query for DbSchemas is not being generated")
}

func TestPrepareTablesSQLWithNoFilter(t *testing.T) {
	catalogNames := [3]string{"DEMO_DB", "DEMOADB", "DEMO'DB"}
	catalogPattern := ""
	schemaPattern := ""
	tableNamePattern := ""
	tableType := make([]string, 0)

	expected := `SELECT table_catalog, table_schema, table_name, table_type, constraint_name, constraint_type FROM (SELECT T.table_catalog, T.table_schema, T.table_name, T.table_type,
			 	TC.constraint_name, TC.constraint_type
				FROM
				(
					"DEMO_DB".INFORMATION_SCHEMA.TABLES AS T
					LEFT JOIN
					"DEMO_DB".INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
					ON
						T.table_catalog = TC.table_catalog
						AND T.table_schema = TC.table_schema
						AND t.table_name = TC.table_name
				) UNION ALL SELECT T.table_catalog, T.table_schema, T.table_name, T.table_type, TC.constraint_name, TC.constraint_type
				FROM
				(
					"DEMOADB".INFORMATION_SCHEMA.TABLES AS T
					LEFT JOIN
					"DEMOADB".INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
					ON
						T.table_catalog = TC.table_catalog
						AND T.table_schema = TC.table_schema
						AND t.table_name = TC.table_name
				) UNION ALL SELECT T.table_catalog, T.table_schema, T.table_name, T.table_type, TC.constraint_name, TC.constraint_type
				FROM
				(
					"DEMO'DB".INFORMATION_SCHEMA.TABLES AS T
					LEFT JOIN
					"DEMO'DB".INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
					ON
						T.table_catalog = TC.table_catalog
						AND T.table_schema = TC.table_schema
						AND t.table_name = TC.table_name
				))`
	actual, queryArgs := prepareTablesSQL(catalogNames[:], &catalogPattern, &schemaPattern, &tableNamePattern, tableType[:])

	println("Query Args", queryArgs)
	assert.True(t, areStringsEquivalent(expected, actual), "The expected SQL query for Tables is not being generated")
}

func TestPrepareTablesSQLWithNoTableTypeFilter(t *testing.T) {
	catalogNames := [3]string{"DEMO_DB", "DEMOADB", "DEMO'DB"}
	catalogPattern := "DEMO_DB"
	schemaPattern := "PUBLIC"
	tableNamePattern := "ADBC-TABLE"
	tableType := make([]string, 0)

	expected := `SELECT table_catalog, table_schema, table_name, table_type, constraint_name, constraint_type FROM (SELECT T.table_catalog, T.table_schema, T.table_name, T.table_type, TC.constraint_name, TC.constraint_type
				FROM
				(
					"DEMO_DB".INFORMATION_SCHEMA.TABLES AS T
					LEFT JOIN
					"DEMO_DB".INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
					ON
						T.table_catalog = TC.table_catalog
						AND T.table_schema = TC.table_schema
						AND t.table_name = TC.table_name
				) UNION ALL SELECT T.table_catalog, T.table_schema, T.table_name, T.table_type, TC.constraint_name, TC.constraint_type
				FROM
				(
					"DEMOADB".INFORMATION_SCHEMA.TABLES AS T
					LEFT JOIN
					"DEMOADB".INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
					ON
						T.table_catalog = TC.table_catalog
						AND T.table_schema = TC.table_schema
						AND t.table_name = TC.table_name
				) UNION ALL SELECT T.table_catalog, T.table_schema, T.table_name, T.table_type, TC.constraint_name, TC.constraint_type
				FROM
				(
					"DEMO'DB".INFORMATION_SCHEMA.TABLES AS T
					LEFT JOIN
					"DEMO'DB".INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
					ON
						T.table_catalog = TC.table_catalog
						AND T.table_schema = TC.table_schema
						AND t.table_name = TC.table_name
				)) WHERE  TABLE_CATALOG ILIKE ?  AND  TABLE_SCHEMA ILIKE ?  AND  TABLE_NAME ILIKE ? `
	actual, queryArgs := prepareTablesSQL(catalogNames[:], &catalogPattern, &schemaPattern, &tableNamePattern, tableType[:])

	stringqueryArgs := make([]string, len(queryArgs)) // Pre-allocate the right size
	for index := range queryArgs {
		stringqueryArgs[index] = fmt.Sprintf("%v", queryArgs[index])
	}

	assert.True(t, areStringsEquivalent(catalogPattern+","+schemaPattern+","+tableNamePattern, strings.Join(stringqueryArgs, ",")), "The expected SCHEMA_NAME is not being generated")
	assert.True(t, areStringsEquivalent(expected, actual), "The expected SQL query for Tables is not being generated")
}

func TestPrepareTablesSQL(t *testing.T) {
	catalogNames := [3]string{"DEMO_DB", "DEMOADB", "DEMO'DB"}
	catalogPattern := "DEMO_DB"
	schemaPattern := "PUBLIC"
	tableNamePattern := "ADBC-TABLE"
	tableType := [2]string{"BASE TABLE", "VIEW"}

	expected := `SELECT table_catalog, table_schema, table_name, table_type, constraint_name, constraint_type FROM (SELECT T.table_catalog, T.table_schema, T.table_name, T.table_type, TC.constraint_name, TC.constraint_type
				FROM
				(
					"DEMO_DB".INFORMATION_SCHEMA.TABLES AS T
					LEFT JOIN
					"DEMO_DB".INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
					ON
						T.table_catalog = TC.table_catalog
						AND T.table_schema = TC.table_schema
						AND t.table_name = TC.table_name
				) UNION ALL SELECT T.table_catalog, T.table_schema, T.table_name, T.table_type, TC.constraint_name, TC.constraint_type
				FROM
				(
					"DEMOADB".INFORMATION_SCHEMA.TABLES AS T
					LEFT JOIN
					"DEMOADB".INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
					ON
						T.table_catalog = TC.table_catalog
						AND T.table_schema = TC.table_schema
						AND t.table_name = TC.table_name
				) UNION ALL SELECT T.table_catalog, T.table_schema, T.table_name, T.table_type, TC.constraint_name, TC.constraint_type
				FROM
				(
					"DEMO'DB".INFORMATION_SCHEMA.TABLES AS T
					LEFT JOIN
					"DEMO'DB".INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
					ON
						T.table_catalog = TC.table_catalog
						AND T.table_schema = TC.table_schema
						AND t.table_name = TC.table_name
				)) WHERE  TABLE_CATALOG ILIKE ?  AND  TABLE_SCHEMA ILIKE ?  AND  TABLE_NAME ILIKE ? AND  TABLE_TYPE IN ('BASE TABLE','VIEW')`
	actual, queryArgs := prepareTablesSQL(catalogNames[:], &catalogPattern, &schemaPattern, &tableNamePattern, tableType[:])

	stringqueryArgs := make([]string, len(queryArgs)) // Pre-allocate the right size
	for index := range queryArgs {
		stringqueryArgs[index] = fmt.Sprintf("%v", queryArgs[index])
	}

	assert.True(t, areStringsEquivalent(catalogPattern+","+schemaPattern+","+tableNamePattern, strings.Join(stringqueryArgs, ",")), "The expected SCHEMA_NAME is not being generated")
	assert.True(t, areStringsEquivalent(expected, actual), "The expected SQL query for Tables is not being generated")
}

func TestPrepareColumnsSQLNoFilter(t *testing.T) {
	catalogNames := [2]string{"DEMO_DB", "DEMOADB"}
	catalogPattern := ""
	schemaPattern := ""
	tableNamePattern := ""
	columnNamePattern := ""
	tableType := make([]string, 0)

	expected := `SELECT table_catalog, table_schema, table_name, column_name,
						ordinal_position, is_nullable::boolean, data_type, numeric_precision,
						numeric_precision_radix, numeric_scale, is_identity::boolean,
						identity_generation, identity_increment, character_maximum_length,
						character_octet_length, datetime_precision, comment
						FROM
						(
							SELECT * FROM "DEMO_DB".INFORMATION_SCHEMA.COLUMNS
							UNION ALL
							SELECT * FROM "DEMOADB".INFORMATION_SCHEMA.COLUMNS
						)
						ORDER BY table_catalog, table_schema, table_name, ordinal_position`
	actual, queryArgs := prepareColumnsSQL(catalogNames[:], &catalogPattern, &schemaPattern, &tableNamePattern, &columnNamePattern, tableType[:])

	println("Query Args", queryArgs)
	assert.True(t, areStringsEquivalent(expected, actual), "The expected SQL query for Tables is not being generated")
}

func TestPrepareColumnsSQL(t *testing.T) {
	catalogNames := [2]string{"DEMO_DB", "DEMOADB"}
	catalogPattern := "DEMO_DB"
	schemaPattern := "PUBLIC"
	tableNamePattern := "ADBC-TABLE"
	columnNamePattern := "creationDate"
	tableType := [2]string{"BASE TABLE", "VIEW"}

	expected := `SELECT table_catalog, table_schema, table_name, column_name,
						ordinal_position, is_nullable::boolean, data_type, numeric_precision,
						numeric_precision_radix, numeric_scale, is_identity::boolean,
						identity_generation, identity_increment, character_maximum_length,
						character_octet_length, datetime_precision, comment
						FROM
						(
							SELECT * FROM "DEMO_DB".INFORMATION_SCHEMA.COLUMNS
							UNION ALL
							SELECT * FROM "DEMOADB".INFORMATION_SCHEMA.COLUMNS
						)
						WHERE  TABLE_CATALOG ILIKE ?  AND  TABLE_SCHEMA ILIKE ?  AND  TABLE_NAME ILIKE ?  AND  COLUMN_NAME ILIKE ?
						ORDER BY table_catalog, table_schema, table_name, ordinal_position`
	actual, queryArgs := prepareColumnsSQL(catalogNames[:], &catalogPattern, &schemaPattern, &tableNamePattern, &columnNamePattern, tableType[:])

	stringqueryArgs := make([]string, len(queryArgs)) // Pre-allocate the right size
	for index := range queryArgs {
		stringqueryArgs[index] = fmt.Sprintf("%v", queryArgs[index])
	}

	assert.True(t, areStringsEquivalent(catalogPattern+","+schemaPattern+","+tableNamePattern+","+columnNamePattern, strings.Join(stringqueryArgs, ",")), "The expected SCHEMA_NAME is not being generated")
	assert.True(t, areStringsEquivalent(expected, actual), "The expected SQL query for Tables is not being generated")
}

func areStringsEquivalent(str1 string, str2 string) bool {
	re := regexp.MustCompile(`\s+`)
	normalizedStr1 := re.ReplaceAllString(str1, "")
	normalizedStr2 := re.ReplaceAllString(str2, "")

	return normalizedStr1 == normalizedStr2
}
