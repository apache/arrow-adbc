# BigQuery OAuth Scopes Enhancement - Implementation Summary

## Overview

This implementation adds support for configuring OAuth 2.0 scopes for BigQuery authentication in the Arrow ADBC Go driver, with clear separation between regular authentication scopes and impersonated service account scopes.

## Problem Statement

Previously, the driver:
- Only supported impersonation scopes (via `OptionStringImpersonateScopes`)
- Had NO way to specify scopes for regular authentication (ADC, JSON credentials, etc.)
- Could not replicate Python's `google.auth.default(scopes=[...])` behavior

## Solution

Added a new configuration option `OptionStringAuthScopes` that:
1. **Clearly separates** regular auth scopes from impersonation scopes
2. **Defaults to null/empty** - no scopes specified by default
3. **Only applies scopes when set** - backward compatible
4. **Works with all auth methods**: ADC, JSON credentials, JSON credential files, user authentication

## Files Modified

### 1. `driver.go`
**Changes:**
- Added constant `OptionStringAuthScopes = "adbc.bigquery.sql.auth.scopes"`
- Added comprehensive documentation

**Lines:** 113-117

### 2. `bigquery_database.go`
**Changes:**
- Added field `authScopes []string` to `databaseImpl` struct (line 38)
- Added to `Open()` method to propagate to connection (line 60)
- Added `GetOption` handler (line 99-100)
- Added `SetOption` handler to parse comma-separated scopes (line 159-160)
- Added `GetOption` handlers for impersonate options (lines 101-106)

**Lines:** 38, 60, 99-100, 101-106, 159-160

### 3. `connection.go`
**Changes:**
- Added field `authScopes []string` to `connectionImpl` struct (line 54)
- Added `GetOption` handler (line 470-471)
- Added `GetOption` handlers for impersonate options (lines 472-477)
- Added `SetOption` handler (line 504-505)
- **Critical:** Modified `newClient()` to apply scopes correctly (lines 597-600)

**Key Logic in `newClient()`:**
```go
// Apply regular authentication scopes if specified (for non-impersonated auth)
if len(c.authScopes) > 0 && !c.hasImpersonationOptions() {
    authOptions = append(authOptions, option.WithScopes(c.authScopes...))
}
```

**Lines:** 54, 470-471, 472-477, 504-505, 597-600

## Key Design Decisions

### 1. Separation of Concerns
- **Regular scopes** (`authScopes`): Used for direct authentication
- **Impersonation scopes** (`impersonateScopes`): Used when impersonating service accounts
- These are mutually exclusive - when impersonation is active, regular scopes are ignored

### 2. Scope Application Logic
```
IF authScopes is set AND NOT impersonating:
    Apply authScopes via option.WithScopes()
ELSE IF impersonating:
    Use impersonateScopes in impersonate.CredentialsConfig
```

### 3. Backward Compatibility
- Default value is empty (no scopes)
- If not set, behavior is identical to previous version
- All existing code continues to work without changes

### 4. Format
- Comma-separated string: `"scope1,scope2,scope3"`
- Internally stored as `[]string`
- Retrieved as comma-separated string via `GetOption`

## Testing

### New Tests in `scopes_test.go`

1. **TestAuthScopesSetGet**: Verifies setting and retrieving scopes
2. **TestAuthScopesSeparateFromImpersonateScopes**: Confirms separation of regular and impersonation scopes
3. **TestAuthScopesEmptyByDefault**: Ensures default is empty (null)
4. **TestAuthScopesCommaSeparated**: Tests multiple scopes handling
5. **TestAuthScopesPropagatedToConnection**: Verifies data flow from database to connection

**All tests pass:** ✅

### Backward Compatibility Testing
- Existing test `TestAuthTypeConsolidation` still passes: ✅
- No breaking changes to existing auth flow

## Usage Example

### Python Equivalent
```python
credentials, project = google.auth.default(
    scopes=[
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/cloud-platform",
    ]
)
client = bigquery.Client(credentials=credentials, project=project)
```

### Go Implementation
```go
drv := bigquery.NewDriver(alloc)
db, err := drv.NewDatabase(map[string]string{
    bigquery.OptionStringProjectID:  "my-project-id",
    bigquery.OptionStringAuthScopes: "https://www.googleapis.com/auth/drive,https://www.googleapis.com/auth/cloud-platform",
})
```

## API Documentation

See `SCOPES_USAGE.md` for comprehensive usage documentation including:
- Configuration examples for all auth types
- Common OAuth scopes reference
- Best practices
- Troubleshooting guide

## Compliance with Requirements

✅ **Regular scopes clearly separated from impersonated service account scopes**
- Separate fields: `authScopes` vs `impersonateScopes`
- Separate options: `OptionStringAuthScopes` vs `OptionStringImpersonateScopes`
- Separate logic paths in `newClient()`

✅ **Scopes specification supported with AND without impersonation**
- `authScopes` works with: ADC, JSON credentials, JSON files, user auth
- `impersonateScopes` works with: impersonation flow

✅ **Default value is null (empty)**
- Default: `[]string{}` (empty slice)
- Not applied unless explicitly set
- Retrieved as `""` (empty string) via `GetOption`

✅ **Only passed to BigQuery client when set**
- Check: `if len(c.authScopes) > 0 && !c.hasImpersonationOptions()`
- Only calls `option.WithScopes()` when scopes exist and not impersonating

## Performance Impact

- **Minimal**: Only adds slice storage and string parsing
- **No impact when not used**: Empty slice check is O(1)
- **No breaking changes**: Existing code paths unchanged

## Security Considerations

- Scopes provide least-privilege access control
- Empty default prevents accidental over-permissioning
- User must explicitly specify scopes for custom access patterns

## Future Enhancements

Potential improvements (not implemented):
1. Scope validation against known Google OAuth scopes
2. Automatic scope recommendation based on operations
3. Scope conflict detection between regular and impersonation modes

## Conclusion

This implementation successfully adds OAuth scope configuration to the BigQuery ADBC driver with:
- ✅ Clear separation of regular and impersonation scopes
- ✅ Full backward compatibility
- ✅ Comprehensive testing
- ✅ Production-ready code quality
- ✅ Complete documentation

The enhancement enables users to configure fine-grained access control for BigQuery operations, matching the functionality available in the Python BigQuery SDK.
