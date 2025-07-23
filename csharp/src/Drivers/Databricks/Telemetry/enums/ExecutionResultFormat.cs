namespace Apache.Arrow.Adbc.Drivers.Databricks.Telemetry.Enums
{
    public enum ExecutionResultFormat
    {
        FORMAT_UNSPECIFIED = 0,
        INLINE_ARROW = 1,
        INLINE_JSON = 2,
        EXTERNAL_LINKS = 3,
        COLUMNAR_INLINE = 4
    }
}