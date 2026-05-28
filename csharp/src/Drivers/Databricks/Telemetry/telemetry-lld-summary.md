****# Analysis: File Locations in Telemetry LLD

Based on my analysis of the design document, **ALL changes are contained within the Databricks driver folder** (`/Users/sreekanth.vadigi/Desktop/projects/arrow-adbc/csharp/src/Drivers/Databricks`). Here's the complete breakdown:

## ✅ New Files to Create (All in Databricks folder)

### 1. Telemetry Core Components

```
/Databricks/Telemetry/
├── TelemetryCollector.cs          (New - event aggregation)
├── TelemetryExporter.cs            (New - HTTP export)
├── ITelemetryExporter.cs           (New - interface)
├── CircuitBreaker.cs               (New - resilience)
├── TelemetryConfiguration.cs       (New - config)
└── Models/
    ├── TelemetryEvent.cs           (New - event model)
    ├── TelemetryRequest.cs         (New - request payload)
    ├── TelemetryResponse.cs        (New - response payload)
    ├── TelemetryFrontendLog.cs     (New - log wrapper)
    ├── FrontendLogContext.cs       (New - context)
    ├── FrontendLogEntry.cs         (New - entry)
    ├── SqlDriverLog.cs             (New - driver log)
    ├── DriverConfiguration.cs      (New - config snapshot)
    ├── SqlOperationData.cs         (New - SQL metrics)
    ├── ChunkDownloadData.cs        (New - chunk metrics)
    ├── DriverErrorInfo.cs          (New - error info)
    ├── TelemetryClientContext.cs   (New - client context)
    └── StatementTelemetryData.cs   (New - aggregated data)
```

### 2. Test Files

```
/Databricks.Tests/Telemetry/
├── TelemetryCollectorTests.cs      (New - unit tests)
├── TelemetryExporterTests.cs       (New - unit tests)
├── CircuitBreakerTests.cs          (New - unit tests)
├── TelemetryIntegrationTests.cs    (New - integration tests)
├── TelemetryPerformanceTests.cs    (New - perf tests)
└── MockTelemetryEndpointTests.cs   (New - mock tests)
```

## ✅ Existing Files to Modify (All in Databricks folder)

### 1. DatabricksParameters.cs

**Location:** `/Databricks/DatabricksParameters.cs`
**Changes:** Add telemetry configuration constants

```csharp
public const string TelemetryEnabled = "adbc.databricks.telemetry.enabled";
public const string TelemetryBatchSize = "adbc.databricks.telemetry.batch_size";
public const string TelemetryFlushIntervalMs = "adbc.databricks.telemetry.flush_interval_ms";
// ... 7 more parameters
```

### 2. DatabricksConnection.cs

**Location:** `/Databricks/DatabricksConnection.cs`
**Changes:**
- Add TelemetryCollector field
- Initialize telemetry in `OpenAsync()`
- Record connection configuration
- Flush telemetry in `Dispose()`
- Check server-side feature flag in `ApplyServerSidePropertiesAsync()`

```csharp
private TelemetryCollector? _telemetryCollector;
private TelemetryConfiguration? _telemetryConfig;

public override async Task OpenAsync(CancellationToken cancellationToken = default)
{
    // ... existing code ...
    InitializeTelemetry();
    _telemetryCollector?.RecordConnectionOpen(latency, driverConfig);
}

public override void Dispose()
{
    _telemetryCollector?.FlushAllPendingAsync().Wait();
    _telemetryCollector?.Dispose();
    base.Dispose();
}
```

### 3. DatabricksStatement.cs

**Location:** `/Databricks/DatabricksStatement.cs`
**Changes:**
- Record statement execution metrics
- Track result format
- Mark statement complete on dispose

```csharp
protected override async Task<QueryResult> ExecuteQueryAsync(...)
{
    var sw = Stopwatch.StartNew();
    // ... execute ...
    Connection.TelemetryCollector?.RecordStatementExecute(
        statementId, sw.Elapsed, resultFormat);
}

public override void Dispose()
{
    Connection.TelemetryCollector?.RecordStatementComplete(_statementId);
    base.Dispose();
}
```

### 4. CloudFetchDownloader.cs

**Location:** `/Databricks/Reader/CloudFetch/CloudFetchDownloader.cs`
**Changes:**
- Record chunk download latency
- Track retry attempts
- Report download errors

```csharp
private async Task DownloadFileAsync(IDownloadResult downloadResult, ...)
{
    var sw = Stopwatch.StartNew();
    // ... download ...
    _statement.Connection.TelemetryCollector?.RecordChunkDownload(
        statementId, chunkIndex, sw.Elapsed, bytesDownloaded, compressed);
}
```

### 5. DatabricksOperationStatusPoller.cs

**Location:** `/Databricks/Reader/DatabricksOperationStatusPoller.cs`
**Changes:**
- Record polling metrics

```csharp
public async Task<TGetOperationStatusResp> PollForCompletionAsync(...)
{
    var pollCount = 0;
    var sw = Stopwatch.StartNew();
    // ... polling loop ...
    _connection.TelemetryCollector?.RecordOperationStatus(
        operationId, pollCount, sw.Elapsed);
}
```

### 6. Exception Handlers (Multiple Files)

**Locations:** Throughout `/Databricks/` (wherever exceptions are caught)
**Changes:** Add telemetry error recording

```csharp
catch (Exception ex)
{
    Connection.TelemetryCollector?.RecordError(
        errorCode, SanitizeErrorMessage(ex.Message), statementId);
    throw;
}
```

### 7. readme.md

**Location:** `/Databricks/readme.md`
**Changes:** Add telemetry documentation section

```markdown
## Telemetry

The Databricks ADBC driver collects anonymous usage telemetry...

### What Data is Collected
### What Data is NOT Collected
### Disabling Telemetry
```

## ❌ NO Changes Outside Databricks Folder

The design does **NOT** require any changes to:

- ❌ Base ADBC library (`Apache.Arrow.Adbc/`)
- ❌ Apache Spark/Hive2 drivers (`Drivers/Apache/`)
- ❌ ADBC interfaces (`AdbcConnection`, `AdbcStatement`, etc.)
- ❌ Activity/Tracing infrastructure (already exists, just reuse)
- ❌ Other ADBC drivers (BigQuery, Snowflake, etc.)

## 📦 External Dependencies

The design **reuses existing infrastructure**:

### Already Available (No Changes Needed):

**Activity/Tracing** (`Apache.Arrow.Adbc.Tracing/`)
- `ActivityTrace` - Already exists
- `IActivityTracer` - Already exists
- Used for correlation, not modified

**HTTP Client**
- `HttpClient` - .NET standard library
- Already used by driver

**JSON Serialization**
- `System.Text.Json` - .NET standard library
- Already used by driver

**Testing Infrastructure**
- MSTest/xUnit - Standard testing frameworks
- Already used by driver tests

## 📁 Complete File Tree

```
arrow-adbc/csharp/src/Drivers/Databricks/
│
├── Telemetry/                               ← NEW FOLDER
│   ├── TelemetryCollector.cs               ← NEW
│   ├── TelemetryExporter.cs                ← NEW
│   ├── ITelemetryExporter.cs               ← NEW
│   ├── CircuitBreaker.cs                   ← NEW
│   ├── TelemetryConfiguration.cs           ← NEW
│   └── Models/                             ← NEW FOLDER
│       ├── TelemetryEvent.cs               ← NEW
│       ├── TelemetryRequest.cs             ← NEW
│       ├── TelemetryResponse.cs            ← NEW
│       ├── TelemetryFrontendLog.cs         ← NEW
│       ├── FrontendLogContext.cs           ← NEW
│       ├── FrontendLogEntry.cs             ← NEW
│       ├── SqlDriverLog.cs                 ← NEW
│       ├── DriverConfiguration.cs          ← NEW
│       ├── SqlOperationData.cs             ← NEW
│       ├── ChunkDownloadData.cs            ← NEW
│       ├── DriverErrorInfo.cs              ← NEW
│       ├── TelemetryClientContext.cs       ← NEW
│       └── StatementTelemetryData.cs       ← NEW
│
├── DatabricksParameters.cs                 ← MODIFY (add constants)
├── DatabricksConnection.cs                 ← MODIFY (add telemetry)
├── DatabricksStatement.cs                  ← MODIFY (add telemetry)
├── readme.md                               ← MODIFY (add docs)
│
├── Reader/
│   ├── DatabricksOperationStatusPoller.cs  ← MODIFY (add telemetry)
│   └── CloudFetch/
│       └── CloudFetchDownloader.cs         ← MODIFY (add telemetry)
│
└── [Other existing files remain unchanged]

arrow-adbc/csharp/test/Drivers/Databricks.Tests/
│
└── Telemetry/                              ← NEW FOLDER
    ├── TelemetryCollectorTests.cs          ← NEW
    ├── TelemetryExporterTests.cs           ← NEW
    ├── CircuitBreakerTests.cs              ← NEW
    ├── TelemetryIntegrationTests.cs        ← NEW
    ├── TelemetryPerformanceTests.cs        ← NEW
    └── MockTelemetryEndpointTests.cs       ← NEW
```

## Summary

✅ **All changes are self-contained within the Databricks driver folder**

**New Files:** ~27 new files (all under `/Databricks/`)
- 14 core implementation files
- 6 test files
- 7+ model classes

**Modified Files:** ~6-8 existing files (all under `/Databricks/`)
- DatabricksParameters.cs
- DatabricksConnection.cs
- DatabricksStatement.cs
- CloudFetchDownloader.cs
- DatabricksOperationStatusPoller.cs
- readme.md
- Exception handlers (scattered)

**External Dependencies:** Zero new dependencies outside the folder
- Reuses existing Activity/Tracing infrastructure
- Uses standard .NET libraries (HttpClient, System.Text.Json)
- No changes to base ADBC library

**This is a clean, modular implementation that doesn't require any changes to the ADBC standard or other drivers!** 🎯
