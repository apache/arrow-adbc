namespace Apache.Arrow.Adbc.Drivers.Databricks.Telemetry.Enums
{
    public enum AuthFlow
    {
        TYPE_UNSPECIFIED = 0,
        TOKEN_PASSTHROUGH = 1,
        CLIENT_CREDENTIALS = 2,
        BROWSER_BASED_AUTHENTICATION = 3
    }
}