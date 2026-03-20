package types

type SqlType = string

// Values match what Salesforce API returns (mixed case).
const (
	SqlTypeVarchar     SqlType = "Varchar"
	SqlTypeChar        SqlType = "Char"
	SqlTypeBigInt      SqlType = "BigInt"
	SqlTypeInteger     SqlType = "Integer"
	SqlTypeSmallInt    SqlType = "SmallInt"
	SqlTypeDouble      SqlType = "Double"
	SqlTypeFloat       SqlType = "Float"
	SqlTypeNumeric     SqlType = "Numeric"
	SqlTypeBool        SqlType = "Bool"
	SqlTypeDate        SqlType = "Date"
	SqlTypeTime        SqlType = "Time"
	SqlTypeTimestamp   SqlType = "Timestamp"
	SqlTypeTimestampTZ SqlType = "TimestampTZ"
	SqlTypeOid         SqlType = "Oid"
	SqlTypeArrayOf     SqlType = "ArrayOfX"
	SqlTypeUnspecified SqlType = "Unspecified"
)

// SqlQueryCompletionStatus represents the status of a SQL query.
type SqlQueryCompletionStatus = string

const (
	SqlQueryFinished        SqlQueryCompletionStatus = "Finished"
	SqlQueryResultsProduced SqlQueryCompletionStatus = "ResultsProduced"
	SqlQueryRunning         SqlQueryCompletionStatus = "Running"
	SqlQueryUnspecified     SqlQueryCompletionStatus = "Unspecified"
)

type SqlQueryStatus struct {
	ChunkCount       int                      `json:"chunkCount"`
	CompletionStatus SqlQueryCompletionStatus `json:"completionStatus"`
	ExpirationTime   string                   `json:"expirationTime,omitempty"`
	Progress         float64                  `json:"progress"`
	QueryID          string                   `json:"queryId"`
	RowCount         int64                    `json:"rowCount"`
}

func (s *SqlQueryStatus) IsFinished() bool {
	return s.CompletionStatus == SqlQueryFinished || s.CompletionStatus == SqlQueryResultsProduced
}

func (s *SqlQueryStatus) IsRunning() bool {
	return s.CompletionStatus == SqlQueryRunning
}

type SqlParameter struct {
	Type  string `json:"type"`
	Name  string `json:"name"`
	Value any    `json:"value"`
}

type SqlQueryOptions struct {
	Dataspace    string
	WorkloadName string
}

type SqlQueryRequest struct {
	SQL           string         `json:"sql"`
	RowLimit      int64          `json:"rowLimit,omitempty"`
	SqlParameters []SqlParameter `json:"sqlParameters,omitempty"`
}

// SqlQueryResponse is the response from Create SQL Query.
// It combines the Get Query status and Get Query Rows data.
type SqlQueryResponse struct {
	Data         [][]any            `json:"data"`
	Metadata     []SqlQueryMetadata `json:"metadata"`
	Status       SqlQueryStatus     `json:"status"`
	ReturnedRows int64              `json:"returnedRows"`
}

// SqlQueryRowsResponse is the response from Get Query Rows.
type SqlQueryRowsResponse struct {
	Data         [][]any            `json:"data"`
	Metadata     []SqlQueryMetadata `json:"metadata"`
	ReturnedRows int64              `json:"returnedRows"`
}

type SqlQueryMetadata struct {
	Name         string  `json:"name"`
	Nullable     bool    `json:"nullable"`
	Type         SqlType `json:"type"`
	InnerElement SqlType `json:"innerElement,omitempty"`
	Precision    *int    `json:"precision,omitempty"`
	Scale        *int    `json:"scale,omitempty"`
}
