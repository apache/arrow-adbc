# BigQuery OAuth Scopes Configuration

This document explains how to configure OAuth 2.0 scopes for BigQuery authentication in the Arrow ADBC Go driver.

## Overview

The driver now supports two distinct types of OAuth scopes:

1. **Regular Authentication Scopes** (`OptionStringAuthScopes`) - Used with standard authentication methods (ADC, JSON credentials, etc.)
2. **Impersonation Scopes** (`OptionStringImpersonateScopes`) - Used when impersonating a service account

These scopes are **completely separate** and serve different purposes.

## Configuration Option

**Option Key:** `adbc.bigquery.sql.auth.scopes`
**Constant:** `driver.OptionStringAuthScopes`
**Format:** Comma-separated list of OAuth 2.0 scope URLs
**Default:** `nil` (empty - no scopes specified)

## Usage Examples

### Example 1: Application Default Credentials (ADC) with Custom Scopes

```go
import (
    "context"
    "github.com/apache/arrow-adbc/go/adbc/driver/bigquery"
    "github.com/apache/arrow-go/v18/arrow/memory"
)

func main() {
    ctx := context.Background()
    alloc := memory.NewGoAllocator()

    drv := bigquery.NewDriver(alloc)

    // Create database with custom scopes for Drive and Cloud Platform access
    db, err := drv.NewDatabase(map[string]string{
        bigquery.OptionStringProjectID: "my-project-id",
        bigquery.OptionStringAuthScopes: "https://www.googleapis.com/auth/drive,https://www.googleapis.com/auth/cloud-platform",
    })
    if err != nil {
        panic(err)
    }
    defer db.Close()

    conn, err := db.Open(ctx)
    if err != nil {
        panic(err)
    }
    defer conn.Close()

    // Use the connection...
}
```

### Example 2: JSON Credentials with Custom Scopes

```go
db, err := drv.NewDatabase(map[string]string{
    bigquery.OptionStringProjectID:       "my-project-id",
    bigquery.OptionStringAuthType:        bigquery.OptionValueAuthTypeJSONCredentialFile,
    bigquery.OptionStringAuthCredentials: "/path/to/service-account.json",
    bigquery.OptionStringAuthScopes:      "https://www.googleapis.com/auth/bigquery,https://www.googleapis.com/auth/drive",
})
```

### Example 3: Impersonated Service Account with Scopes

When using impersonation, use `OptionStringImpersonateScopes` instead:

```go
db, err := drv.NewDatabase(map[string]string{
    bigquery.OptionStringProjectID:                    "my-project-id",
    bigquery.OptionStringImpersonateTargetPrincipal:   "sa@my-project.iam.gserviceaccount.com",
    bigquery.OptionStringImpersonateScopes:            "https://www.googleapis.com/auth/bigquery,https://www.googleapis.com/auth/cloud-platform",
    bigquery.OptionStringImpersonateLifetime:          "3600s",
})
```

**Important:** When impersonation is active, `OptionStringAuthScopes` is ignored. Only `OptionStringImpersonateScopes` is used.

### Example 4: Python-Style Usage Equivalent

The following Python code:

```python
credentials, project = google.auth.default(
    scopes=[
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/cloud-platform",
    ]
)
client = bigquery.Client(credentials=credentials, project=project)
```

Is equivalent to this Go code:

```go
db, err := drv.NewDatabase(map[string]string{
    bigquery.OptionStringProjectID:  "my-project-id",
    bigquery.OptionStringAuthScopes: "https://www.googleapis.com/auth/drive,https://www.googleapis.com/auth/cloud-platform",
})
```

## Common OAuth Scopes for BigQuery

| Scope URL | Description |
|-----------|-------------|
| `https://www.googleapis.com/auth/bigquery` | Full access to BigQuery |
| `https://www.googleapis.com/auth/bigquery.readonly` | Read-only access to BigQuery |
| `https://www.googleapis.com/auth/cloud-platform` | Full access to all Google Cloud resources |
| `https://www.googleapis.com/auth/drive` | Access to Google Drive (for external tables) |
| `https://www.googleapis.com/auth/devstorage.read_write` | Access to Google Cloud Storage |

## Technical Details

### How It Works

1. **Regular Authentication (Non-Impersonated):**
   - When `authScopes` is set and impersonation is NOT active
   - Scopes are passed to BigQuery client via `option.WithScopes()`
   - Works with: ADC, JSON credentials, JSON credential files

2. **Impersonated Authentication:**
   - When `impersonateTargetPrincipal` is set
   - Uses `impersonate.CredentialsConfig.Scopes`
   - `authScopes` is ignored in this mode

### Implementation Details

The scope handling logic in `connection.go` (`newClient()` method):

```go
// Apply regular authentication scopes if specified (for non-impersonated auth)
if len(c.authScopes) > 0 && !c.hasImpersonationOptions() {
    authOptions = append(authOptions, option.WithScopes(c.authScopes...))
}

// Then, apply impersonation if configured (as a credential transformation layer)
if c.hasImpersonationOptions() {
    impCfg := impersonate.CredentialsConfig{
        TargetPrincipal: c.impersonateTargetPrincipal,
        Delegates:       c.impersonateDelegates,
        Scopes:          c.impersonateScopes,  // Uses impersonateScopes
        Lifetime:        lifetime,
    }
    // ...
}
```

## Best Practices

1. **Default Behavior:** If you don't set `OptionStringAuthScopes`, the driver uses Google's default scopes for the authentication method.

2. **Minimum Scopes:** Always include the minimum required scopes for your use case to follow the principle of least privilege.

3. **Drive Access:** If you're accessing external tables stored in Google Drive, include the Drive scope:
   ```go
   "https://www.googleapis.com/auth/drive,https://www.googleapis.com/auth/cloud-platform"
   ```

4. **Read-Only Access:** For read-only workloads, use `bigquery.readonly`:
   ```go
   bigquery.OptionStringAuthScopes: "https://www.googleapis.com/auth/bigquery.readonly"
   ```

5. **Testing:** Test your scope configuration with the minimum permissions needed for your application.

## Troubleshooting

### Error: "Request had insufficient authentication scopes"

**Cause:** The scopes you provided don't include the necessary permissions.

**Solution:** Add the required scope. For most BigQuery operations:
```go
bigquery.OptionStringAuthScopes: "https://www.googleapis.com/auth/cloud-platform"
```

### Scopes Not Being Applied

**Cause:** You might be using impersonation. When impersonation is active, `authScopes` is ignored.

**Solution:** Use `OptionStringImpersonateScopes` instead when impersonating.

## Changes Summary

### Files Modified

1. **`driver.go`**: Added `OptionStringAuthScopes` constant
2. **`bigquery_database.go`**: Added `authScopes` field and Get/SetOption handlers
3. **`connection.go`**: Added `authScopes` field, Get/SetOption handlers, and scope application logic in `newClient()`

### Backward Compatibility

This change is **100% backward compatible**:
- If `OptionStringAuthScopes` is not set, behavior is unchanged
- Existing code continues to work without modifications
- Default value is empty (no scopes), which uses Google's defaults
