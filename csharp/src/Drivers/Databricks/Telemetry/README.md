# Databricks Telemetry

Uses `DatabricksActivityListener.cs` to listen and record events to `TelemetryHelper.cs` using ActivityListener.

`TelemetryHelper.cs` stores telemetry logs and telemetry parameters.

`TelemtryClient.cs` sends logs to `/telemetry` or `/telemetry-unauth` depending on authentication level.
