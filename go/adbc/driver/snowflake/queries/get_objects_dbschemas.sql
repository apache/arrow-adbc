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
