package databricks

import (
    "regexp"
    "strings"

    "github.com/apache/arrow-adbc/go/adbc"
)

const unknownVendorCode = int32(-1) // sentinel value for vendor code; databricks doesn't populate

var (
    sqlstateRE               = regexp.MustCompile(`(?i)SQLSTATE:\s*([A-Z0-9]{5})`)
    defaultSQLStateErrorCode = [5]byte{'H', 'Y', '0', '0', '0'} // general error
)

func NewAdbcError(msg string, code adbc.Status) adbc.Error {
    sqlstate := defaultSQLStateErrorCode

    if m := sqlstateRE.FindStringSubmatch(msg); len(m) == 2 {
        copy(sqlstate[:], strings.ToUpper(m[1]))
    }

    return adbc.Error{
        Msg:        msg,
        Code:       code,
        VendorCode: unknownVendorCode,
        SqlState:   sqlstate,
        Details:    nil,
    }
}
