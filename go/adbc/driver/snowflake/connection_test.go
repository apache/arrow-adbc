package snowflake

import (
	"regexp"
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
	actual := prepareDbSchemasSQL(catalogNames[:], &catalogPattern, &schemaPattern)

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
	actual := prepareDbSchemasSQL(catalogNames[:], &catalogPattern, &schemaPattern)

	assert.True(t, areStringsEquivalent(expected, actual), "The expected SQL query for DbSchemas is not being generated")
}

func TestPrepareDbSchemasSQLWithCatalogFilter(t *testing.T) {
	//prepareDbSchemasSQL(catalogNames []string, catalog *string, dbSchema *string)
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
					WHERE  CATALOG_NAME ILIKE 'DEMO_DB'`
	actual := prepareDbSchemasSQL(catalogNames[:], &catalogPattern, &schemaPattern)

	assert.True(t, areStringsEquivalent(expected, actual), "The expected SQL query for DbSchemas is not being generated")
}

func TestPrepareDbSchemasSQLWithSchemaFilter(t *testing.T) {
	//prepareDbSchemasSQL(catalogNames []string, catalog *string, dbSchema *string)
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
					WHERE  SCHEMA_NAME ILIKE 'PUBLIC'`
	actual := prepareDbSchemasSQL(catalogNames[:], &catalogPattern, &schemaPattern)

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
					WHERE  CATALOG_NAME ILIKE 'DEMO_DB' AND  SCHEMA_NAME ILIKE 'PUBLIC'`
	actual := prepareDbSchemasSQL(catalogNames[:], &catalogPattern, &schemaPattern)

	assert.True(t, areStringsEquivalent(expected, actual), "The expected SQL query for DbSchemas is not being generated")
}

func TestPrepareTablesSQLWithNoFilter(t *testing.T) {
	catalogNames := [3]string{"DEMO_DB", "DEMOADB", "DEMO'DB"}
	catalogPattern := ""
	schemaPattern := ""
	tableNamePattern := ""
	tableType := make([]string, 0)

	expected := `SELECT table_catalog, table_schema, table_name, table_type 
					FROM 
					(
						SELECT * FROM "DEMO_DB".INFORMATION_SCHEMA.TABLES
						UNION ALL 
						SELECT * FROM "DEMOADB".INFORMATION_SCHEMA.TABLES
						UNION ALL 
						SELECT * FROM "DEMO'DB".INFORMATION_SCHEMA.TABLES
					)`
	actual := prepareTablesSQL(catalogNames[:], &catalogPattern, &schemaPattern, &tableNamePattern, tableType[:])

	assert.True(t, areStringsEquivalent(expected, actual), "The expected SQL query for Tables is not being generated")
}

func TestPrepareTablesSQLWithNoTableTypeFilter(t *testing.T) {
	catalogNames := [3]string{"DEMO_DB", "DEMOADB", "DEMO'DB"}
	catalogPattern := "DEMO_DB"
	schemaPattern := "PUBLIC"
	tableNamePattern := "ADBC-TABLE"
	tableType := make([]string, 0)

	expected := `SELECT table_catalog, table_schema, table_name, table_type
					FROM 
					(
						SELECT * FROM "DEMO_DB".INFORMATION_SCHEMA.TABLES
						UNION ALL 
						SELECT * FROM "DEMOADB".INFORMATION_SCHEMA.TABLES
						UNION ALL 
						SELECT * FROM "DEMO'DB".INFORMATION_SCHEMA.TABLES
					)
					WHERE  TABLE_CATALOG ILIKE 'DEMO_DB' AND  TABLE_SCHEMA ILIKE 'PUBLIC' AND  TABLE_NAME ILIKE 'ADBC-TABLE'`
	actual := prepareTablesSQL(catalogNames[:], &catalogPattern, &schemaPattern, &tableNamePattern, tableType[:])

	assert.True(t, areStringsEquivalent(expected, actual), "The expected SQL query for Tables is not being generated")
}

func TestPrepareTablesSQL(t *testing.T) {
	catalogNames := [3]string{"DEMO_DB", "DEMOADB", "DEMO'DB"}
	catalogPattern := "DEMO_DB"
	schemaPattern := "PUBLIC"
	tableNamePattern := "ADBC-TABLE"
	tableType := [2]string{"BASE TABLE", "VIEW"}

	expected := `SELECT table_catalog, table_schema, table_name, table_type
					FROM 
					(
						SELECT * FROM "DEMO_DB".INFORMATION_SCHEMA.TABLES
						UNION ALL 
						SELECT * FROM "DEMOADB".INFORMATION_SCHEMA.TABLES
						UNION ALL 
						SELECT * FROM "DEMO'DB".INFORMATION_SCHEMA.TABLES
					)
					WHERE  TABLE_CATALOG ILIKE 'DEMO_DB' AND  TABLE_SCHEMA ILIKE 'PUBLIC' AND  TABLE_NAME ILIKE 'ADBC-TABLE' AND  TABLE_TYPE IN ('BASE TABLE','VIEW')`
	actual := prepareTablesSQL(catalogNames[:], &catalogPattern, &schemaPattern, &tableNamePattern, tableType[:])

	assert.True(t, areStringsEquivalent(expected, actual), "The expected SQL query for Tables is not being generated")
}

func TestPrepareColumnsSQLNoFilter(t *testing.T) {
	catalogNames := [2]string{"DEMO_DB", "DEMOADB"}
	catalogPattern := ""
	schemaPattern := ""
	tableNamePattern := ""
	columnNamePattern := ""
	tableType := make([]string, 0)

	expected := `SELECT table_type, table_catalog, table_schema, table_name, column_name,
						ordinal_position, is_nullable::boolean, data_type, numeric_precision,
						numeric_precision_radix, numeric_scale, is_identity::boolean,
						identity_generation, identity_increment,
						character_maximum_length, character_octet_length, datetime_precision, comment 
						FROM 
						(
							SELECT T.table_type, C.*
							FROM 
								"DEMO_DB".INFORMATION_SCHEMA.TABLES AS T
								JOIN
								"DEMO_DB".INFORMATION_SCHEMA.COLUMNS AS C
							ON
								T.table_catalog = C.table_catalog AND T.table_schema = C.table_schema AND t.table_name = C.table_name
							UNION ALL
							SELECT T.table_type, C.*
							FROM
								"DEMOADB".INFORMATION_SCHEMA.TABLES AS T
								JOIN 
								"DEMOADB".INFORMATION_SCHEMA.COLUMNS AS C
							ON
								T.table_catalog = C.table_catalog AND T.table_schema = C.table_schema AND t.table_name = C.table_name
						) 
						ORDER BY table_catalog, table_schema, table_name, ordinal_position`
	actual := prepareColumnsSQL(catalogNames[:], &catalogPattern, &schemaPattern, &tableNamePattern, &columnNamePattern, tableType[:])

	assert.True(t, areStringsEquivalent(expected, actual), "The expected SQL query for Tables is not being generated")
}

func TestPrepareColumnsSQL(t *testing.T) {
	catalogNames := [2]string{"DEMO_DB", "DEMOADB"}
	catalogPattern := "DEMO_DB"
	schemaPattern := "PUBLIC"
	tableNamePattern := "ADBC-TABLE"
	columnNamePattern := "creationDate"
	tableType := [2]string{"BASE TABLE", "VIEW"}

	expected := `SELECT table_type, table_catalog, table_schema, table_name, column_name,
						ordinal_position, is_nullable::boolean, data_type, numeric_precision,
						numeric_precision_radix, numeric_scale, is_identity::boolean,
						identity_generation, identity_increment,
						character_maximum_length, character_octet_length, datetime_precision, comment 
						FROM 
						(
							SELECT T.table_type, C.*
							FROM 
								"DEMO_DB".INFORMATION_SCHEMA.TABLES AS T
								JOIN
								"DEMO_DB".INFORMATION_SCHEMA.COLUMNS AS C
							ON
								T.table_catalog = C.table_catalog AND T.table_schema = C.table_schema AND t.table_name = C.table_name
							UNION ALL
							SELECT T.table_type, C.*
							FROM
								"DEMOADB".INFORMATION_SCHEMA.TABLES AS T
								JOIN 
								"DEMOADB".INFORMATION_SCHEMA.COLUMNS AS C
							ON
								T.table_catalog = C.table_catalog AND T.table_schema = C.table_schema AND t.table_name = C.table_name
						) 
						WHERE  TABLE_CATALOG ILIKE 'DEMO_DB' AND  TABLE_SCHEMA ILIKE 'PUBLIC' AND  TABLE_NAME ILIKE 'ADBC-TABLE' AND  COLUMN_NAME ILIKE 'creationDate' AND  TABLE_TYPE IN ('BASE TABLE','VIEW') 
						ORDER BY table_catalog, table_schema, table_name, ordinal_position`
	actual := prepareColumnsSQL(catalogNames[:], &catalogPattern, &schemaPattern, &tableNamePattern, &columnNamePattern, tableType[:])

	assert.True(t, areStringsEquivalent(expected, actual), "The expected SQL query for Tables is not being generated")
}

func areStringsEquivalent(str1 string, str2 string) bool {
	re := regexp.MustCompile(`\s+`)
	normalizedStr1 := re.ReplaceAllString(str1, "")
	normalizedStr2 := re.ReplaceAllString(str2, "")

	return normalizedStr1 == normalizedStr2
}
