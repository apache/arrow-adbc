package api

import (
	"context"
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc/driver/salesforce/api/types"
)

// CreateDataLakeObject creates a new Data Lake Object.
func (c *Client) CreateDataLakeObject(ctx context.Context, req *types.DataLakeObjectRequest) (*types.DataLakeObject, error) {
	var result types.DataLakeObject
	resp, err := c.request(ctx).SetBody(req).SetResult(&result).
		Post("/services/data/{version}/ssot/data-lake-objects")
	if err != nil {
		return nil, fmt.Errorf("create DLO request failed: %w", err)
	}
	if resp.IsError() {
		return nil, checkError(resp)
	}
	return &result, nil
}

// GetDataLakeObject retrieves a Data Lake Object by name or ID.
// The API returns a wrapper with a list; this returns the first match.
func (c *Client) GetDataLakeObject(ctx context.Context, nameOrID string) (*types.DataLakeObject, error) {
	var result types.DataLakeObjectCollection
	resp, err := c.request(ctx).
		SetPathParam("nameOrID", nameOrID).
		SetResult(&result).
		Get("/services/data/{version}/ssot/data-lake-objects/{nameOrID}")
	if err != nil {
		return nil, fmt.Errorf("get DLO request failed: %w", err)
	}
	if resp.IsError() {
		return nil, checkError(resp)
	}
	if len(result.Items) == 0 {
		return nil, &SalesforceError{
			StatusCode: 404,
			Code:       "NOT_FOUND",
			Message:    fmt.Sprintf("no DLO found with name or ID %s", nameOrID),
		}
	}
	return &result.Items[0], nil
}

// DeleteDataLakeObject deletes a Data Lake Object by name or ID.
func (c *Client) DeleteDataLakeObject(ctx context.Context, nameOrID string) error {
	resp, err := c.request(ctx).
		SetPathParam("nameOrID", nameOrID).
		Delete("/services/data/{version}/ssot/data-lake-objects/{nameOrID}")
	if err != nil {
		return fmt.Errorf("delete DLO request failed: %w", err)
	}
	if resp.IsError() {
		return checkError(resp)
	}
	return nil
}
