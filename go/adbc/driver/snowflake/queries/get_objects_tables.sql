WITH constraints AS (
    SELECT
        table_catalog,
        table_schema,
        table_name,
        ARRAY_AGG({
            'constraint_name': constraint_name,
            'constraint_type': constraint_type
        }) table_constraints,
    FROM information_schema.table_constraints
    WHERE table_catalog ILIKE :CATALOG AND table_schema ILIKE :DB_SCHEMA AND table_name ILIKE :TABLE
    GROUP BY table_catalog, table_schema, table_name
),
tables AS (
SELECT
    table_catalog catalog_name,
    table_schema schema_name,
    ARRAY_AGG({
        'table_name': table_name,
        'table_type': table_type,
        'table_constraints': table_constraints,
        'table_columns': null
    }) db_schema_tables
FROM information_schema.tables
LEFT JOIN constraints
USING (table_catalog, table_schema, table_name)
WHERE table_catalog ILIKE :CATALOG AND table_schema ILIKE :DB_SCHEMA AND table_name ILIKE :TABLE
GROUP BY table_catalog, table_schema
),
db_schemas AS (
    SELECT
        catalog_name,
        schema_name,
        db_schema_tables,
    FROM information_schema.schemata
    LEFT JOIN tables
    USING (catalog_name, schema_name)
    WHERE catalog_name ILIKE :CATALOG AND schema_name ILIKE :DB_SCHEMA
)
SELECT
    {
        'catalog_name': database_name,
        'catalog_db_schemas': ARRAY_AGG({
            'db_schema_name': schema_name,
            'db_schema_tables': db_schema_tables
        })
    } get_objects
FROM
    information_schema.databases
LEFT JOIN db_schemas
ON database_name = catalog_name
WHERE database_name ILIKE :CATALOG
GROUP BY database_name;
