package api

import (
	"context"
	"fmt"
	"strconv"

	"github.com/apache/arrow-adbc/go/adbc/driver/salesforce/api/types"
	"resty.dev/v3"
)

// CreateSqlQuery creates and executes a SQL query, returning results inline.
// For large result sets, use GetSqlQueryStatus to poll and GetSqlQueryRows to paginate.
func (c *Client) CreateSqlQuery(ctx context.Context, req *types.SqlQueryRequest, opts *types.SqlQueryOptions) (*types.SqlQueryResponse, error) {
	r := c.request(ctx).SetBody(req)
	applySqlQueryOptions(r, opts)

	var result types.SqlQueryResponse
	resp, err := r.SetResult(&result).Post("/services/data/{version}/ssot/query-sql")
	if err != nil {
		return nil, fmt.Errorf("sql query request failed: %w", err)
	}
	if resp.IsError() {
		return nil, checkError(resp)
	}

	return &result, nil
}

// GetSqlQueryStatus retrieves the status of a previously created SQL query.
// Use waitTimeMs to long-poll (max 10000ms); 0 returns immediately.
func (c *Client) GetSqlQueryStatus(ctx context.Context, queryID string, waitTimeMs int, opts *types.SqlQueryOptions) (*types.SqlQueryStatus, error) {
	r := c.request(ctx).SetPathParam("queryID", queryID)
	if waitTimeMs > 0 {
		r.SetQueryParam("waitTimeMs", strconv.Itoa(waitTimeMs))
	}
	applySqlQueryOptions(r, opts)

	var result types.SqlQueryStatus
	resp, err := r.SetResult(&result).Get("/services/data/{version}/ssot/query-sql/{queryID}")
	if err != nil {
		return nil, fmt.Errorf("get query status failed: %w", err)
	}
	if resp.IsError() {
		return nil, checkError(resp)
	}

	return &result, nil
}

// GetSqlQueryRows retrieves rows from a completed SQL query.
// Use offset and rowLimit to paginate through large result sets.
func (c *Client) GetSqlQueryRows(ctx context.Context, queryID string, offset int64, rowLimit int64, opts *types.SqlQueryOptions) (*types.SqlQueryRowsResponse, error) {
	r := c.request(ctx).
		SetPathParam("queryID", queryID).
		SetQueryParam("offset", strconv.FormatInt(offset, 10))
	if rowLimit > 0 {
		r.SetQueryParam("rowLimit", strconv.FormatInt(rowLimit, 10))
	}
	applySqlQueryOptions(r, opts)

	var result types.SqlQueryRowsResponse
	resp, err := r.SetResult(&result).Get("/services/data/{version}/ssot/query-sql/{queryID}/rows")
	if err != nil {
		return nil, fmt.Errorf("get query rows failed: %w", err)
	}
	if resp.IsError() {
		return nil, checkError(resp)
	}

	return &result, nil
}

// CancelSqlQuery cancels a running SQL query.
func (c *Client) CancelSqlQuery(ctx context.Context, queryID string) error {
	resp, err := c.request(ctx).
		SetPathParam("queryID", queryID).
		Delete("/services/data/{version}/ssot/query-sql/{queryID}")
	if err != nil {
		return fmt.Errorf("cancel query failed: %w", err)
	}
	if resp.IsError() {
		return checkError(resp)
	}

	return nil
}

// applySqlQueryOptions sets common query params on a resty request.
func applySqlQueryOptions(r *resty.Request, opts *types.SqlQueryOptions) {
	if opts == nil {
		return
	}
	if opts.Dataspace != "" {
		r.SetQueryParam("dataspace", opts.Dataspace)
	}
	if opts.WorkloadName != "" {
		r.SetQueryParam("workloadName", opts.WorkloadName)
	}
}
