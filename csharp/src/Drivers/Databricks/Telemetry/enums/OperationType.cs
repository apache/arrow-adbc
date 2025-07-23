namespace Apache.Arrow.Adbc.Drivers.Databricks.Telemetry.Enums
{
    public enum OperationType
    {
        TYPE_UNSPECIFIED = 0,
        CREATE_SESSION = 1,
        DELETE_SESSION = 2,
        EXECUTE_STATEMENT = 3,
        EXECUTE_STATEMENT_ASYNC = 4,
        CLOSE_STATEMENT = 5,
        CANCEL_STATEMENT = 6,
        LIST_TYPE_INFO = 7,
        LIST_CATALOGS = 8,
        LIST_SCHEMAS = 9,
        LIST_TABLES = 10,
        LIST_TABLE_TYPES = 11,
        LIST_COLUMNS = 12,
        LIST_FUNCTIONS = 13,
        LIST_PRIMARY_KEYS = 14,
        LIST_IMPORTED_KEYS = 15,
        LIST_EXPORTED_KEYS = 16,
        LIST_CROSS_REFERENCES = 17
    }
}