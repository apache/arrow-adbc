namespace Apache.Arrow.Adbc.Drivers.Databricks.Telemetry.Enums
{
    public enum StatementType
    {
        TYPE_UNSPECIFIED = 0,
        QUERY = 1,
        SQL = 2,
        UPDATE = 3,
        METADATA = 4,
        VOLUME = 5
    }
}