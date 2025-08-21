# PowerBI ADBC Driver Logging Guide

This guide shows how to enable comprehensive logging when using the ADBC Databricks driver with PowerBI.

## ⚡ Quick Start

**Logging is now automatically enabled** - no connection string changes needed!

**Log location:** `%LOCALAPPDATA%\Apache.Arrow.Adbc\Traces\`
**Contains:** HTTP requests, SQL execution, CloudFetch downloads, TCP connections, memory usage, errors

## 🎯 OpenTelemetry Structured Tracing (Built-in)

### Connection String Configuration
**No additional configuration needed!** Logging is automatically active.

Your standard PowerBI connection string:
```
Server=your-server.com;
Port=443;
UID=token;
PWD=your-access-token
```

### Log File Location
Logs are automatically written to:
- **Windows**: `%LOCALAPPDATA%\Apache.Arrow.Adbc\Traces\`
- **macOS**: `$HOME/Library/Application Support/Apache.Arrow.Adbc/Traces/`
- **Linux**: `$HOME/.local/share/Apache.Arrow.Adbc/Traces/`

### Log File Format
Files are named: `apache.arrow.adbc.drivers.databricks-YYYY-MM-DD-HH-mm-ss-fff-processid.log`

Example: `apache.arrow.adbc.drivers.databricks-2025-08-20-14-30-15-123-12345.log`

### Example PowerBI Connection Strings

#### Standard Connection:
```
Server=your-server.com;
Port=443;
UID=token;
PWD=your-token
```

#### With CloudFetch Performance Tuning:
```
Server=your-server.com;
Port=443;
UID=token;
PWD=your-token;
adbc.databricks.use_cloud_fetch=true;
adbc.databricks.cloudfetch.parallel_downloads=10;
adbc.databricks.cloudfetch.memory_buffer_mb=700
```

## 📄 Log Content

### OpenTelemetry Traces Include:
- **HTTP Requests**: Request/response details, headers, timing
- **SQL Execution**: Query planning, execution traces, result processing
- **Connection Lifecycle**: Connection establishment, authentication, teardown
- **CloudFetch Operations**: Download queue management, TCP connection counts, memory buffer usage
- **Performance Metrics**: Download timing, thread assignments, ServicePoint connection limits
- **Error Analysis**: Stack traces, exception details, failure context

## 🔍 Troubleshooting

### If logs aren't appearing:
1. **Check permissions**: Ensure PowerBI can write to `%LOCALAPPDATA%\Apache.Arrow.Adbc\Traces\`
2. **Check Windows Event Log**: Look for ADBC-related errors
3. **Verify directory exists**: Create the Traces directory manually if needed
4. **Check PowerBI version**: Ensure you're using a compatible ADBC driver version

### Custom log file location:
You can customize the log location by setting environment variables:
```
ADBC_TRACE_LOCATION=C:\your\custom\path\
```

## 📊 Log Analysis for CloudFetch Issues

Look for these trace patterns:

### Connection Degradation:
```json
{"message": "ServicePoint ConnectionLimit OK: 10 (needed 10)", "level": "Information"}
{"message": "SLOW ADD File took 8000ms (QUEUE FULL)", "level": "Warning"}
```

### Memory Pressure:
```json
{"message": "File acquired 680MB memory", "level": "Information"}
{"message": "Memory usage: 680MB / 700MB (97% used)", "level": "Warning"}
```

### Performance Analysis:
```json
{"message": "Thread 15 downloading at 14:30:15.123", "level": "Information"}
{"message": "Queue count: ~45/50 (near capacity)", "level": "Information"}
```
