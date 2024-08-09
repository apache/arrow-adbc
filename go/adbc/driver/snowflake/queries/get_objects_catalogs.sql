SELECT
    {
        'catalog_name': database_name,
        'catalog_db_schemas': null
    } get_objects
FROM
    information_schema.databases
WHERE database_name ILIKE :CATALOG;
