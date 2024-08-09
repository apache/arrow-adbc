WITH columns AS (
    SELECT
        table_catalog,
        table_schema,
        table_name,
        ARRAY_AGG({
        	'column_name': column_name,
        	'ordinal_position': ordinal_position,
        	'remarks': comment,
        	'xdbc_type_name': data_type,
        	'xdbc_is_nullable': is_nullable,
        	'xdbc_nullable': is_nullable::boolean::int,
        	'xdbc_column_size': coalesce(character_maximum_length, numeric_precision),
        	'xdbc_char_octet_length': character_octet_length,
        	'xdbc_decimal_digits': numeric_scale,
        	'xdbc_num_prec_radix': numeric_precision_radix,
        	'xdbc_datetime_sub': datetime_precision
        }) table_columns,
    FROM information_schema.columns
    WHERE table_catalog ILIKE :CATALOG AND table_schema ILIKE :DB_SCHEMA AND table_name ILIKE :TABLE AND column_name ILIKE :COLUMN
    GROUP BY table_catalog, table_schema, table_name
),
constraints AS (
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
        'table_columns': table_columns,
        'table_constraints': table_constraints
    }) db_schema_tables
FROM information_schema.tables
LEFT JOIN columns
USING (table_catalog, table_schema, table_name)
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
