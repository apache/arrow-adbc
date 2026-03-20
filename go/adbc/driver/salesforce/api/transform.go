package api

import (
	"context"
	"errors"
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc/driver/salesforce/api/types"
)

func (c *Client) ListDataTransforms(ctx context.Context) (*types.DataTransformCollection, error) {
	var result types.DataTransformCollection
	resp, err := c.request(ctx).SetResult(&result).
		Get("/services/data/{version}/ssot/data-transforms")
	if err != nil {
		return nil, fmt.Errorf("list data transforms request failed: %w", err)
	}
	if resp.IsError() {
		return nil, checkError(resp)
	}
	return &result, nil
}

func (c *Client) CreateDataTransform(ctx context.Context, req *types.DataTransformRequest) (*types.DataTransform, error) {
	var result types.DataTransform
	resp, err := c.request(ctx).SetBody(req).SetResult(&result).
		Post("/services/data/{version}/ssot/data-transforms")
	if err != nil {
		return nil, fmt.Errorf("create data transform request failed: %w", err)
	}
	if resp.IsError() {
		return nil, checkError(resp)
	}
	return &result, nil
}

func (c *Client) CreateOrUpdateDataTransform(ctx context.Context, req *types.DataTransformRequest) (*types.DataTransform, error) {
	dt, err := c.UpdateDataTransform(ctx, req)
	if err == nil {
		return dt, nil
	}
	// If update failed (transform doesn't exist), fall back to create
	var sfErr *SalesforceError
	if errors.As(err, &sfErr) && sfErr.IsNotFound() {
		return c.CreateDataTransform(ctx, req)
	}
	return nil, err
}

func (c *Client) GetDataTransform(ctx context.Context, nameOrID string) (*types.DataTransform, error) {
	var result types.DataTransform
	resp, err := c.request(ctx).
		SetPathParam("nameOrID", nameOrID).
		SetResult(&result).
		Get("/services/data/{version}/ssot/data-transforms/{nameOrID}")
	if err != nil {
		return nil, fmt.Errorf("get data transform request failed: %w", err)
	}
	if resp.IsError() {
		return nil, checkError(resp)
	}
	return &result, nil
}

func (c *Client) UpdateDataTransform(ctx context.Context, req *types.DataTransformRequest) (*types.DataTransform, error) {
	var result types.DataTransform
	resp, err := c.request(ctx).
		SetPathParam("nameOrID", req.Name).
		SetBody(req).
		SetResult(&result).
		Put("/services/data/{version}/ssot/data-transforms/{nameOrID}")
	if err != nil {
		return nil, fmt.Errorf("update data transform request failed: %w", err)
	}
	if resp.IsError() {
		return nil, checkError(resp)
	}
	return &result, nil
}

func (c *Client) DeleteDataTransform(ctx context.Context, nameOrID string) error {
	resp, err := c.request(ctx).
		SetPathParam("nameOrID", nameOrID).
		Delete("/services/data/{version}/ssot/data-transforms/{nameOrID}")
	if err != nil {
		return fmt.Errorf("delete data transform request failed: %w", err)
	}
	if resp.IsError() {
		return checkError(resp)
	}
	return nil
}

func (c *Client) ValidateDataTransform(ctx context.Context, req *types.DataTransformRequest) (*types.DataTransformValidation, error) {
	var result types.DataTransformValidation
	resp, err := c.request(ctx).SetBody(req).SetResult(&result).
		Post("/services/data/{version}/ssot/data-transforms-validation")
	if err != nil {
		return nil, fmt.Errorf("validate data transform request failed: %w", err)
	}
	if resp.IsError() {
		return nil, checkError(resp)
	}
	return &result, nil
}

func (c *Client) RetryDataTransform(ctx context.Context, nameOrID string) (*types.DataTransform, error) {
	var result types.DataTransform
	resp, err := c.request(ctx).
		SetPathParam("nameOrID", nameOrID).
		SetResult(&result).
		Post("/services/data/{version}/ssot/data-transforms/{nameOrID}/actions/retry")
	if err != nil {
		return nil, fmt.Errorf("retry data transform request failed: %w", err)
	}
	if resp.IsError() {
		return nil, checkError(resp)
	}
	return &result, nil
}

func (c *Client) RunDataTransform(ctx context.Context, nameOrID string) (*types.DataCloudActionResponse, error) {
	var result types.DataCloudActionResponse
	resp, err := c.request(ctx).
		SetPathParam("nameOrID", nameOrID).
		SetResult(&result).
		Post("/services/data/{version}/ssot/data-transforms/{nameOrID}/actions/run")
	if err != nil {
		return nil, fmt.Errorf("run data transform request failed: %w", err)
	}
	if resp.IsError() {
		return nil, checkError(resp)
	}
	return &result, nil
}

func (c *Client) CancelDataTransform(ctx context.Context, nameOrID string) (*types.DataCloudActionResponse, error) {
	var result types.DataCloudActionResponse
	resp, err := c.request(ctx).
		SetPathParam("nameOrID", nameOrID).
		SetResult(&result).
		Post("/services/data/{version}/ssot/data-transforms/{nameOrID}/actions/cancel")
	if err != nil {
		return nil, fmt.Errorf("cancel data transform request failed: %w", err)
	}
	if resp.IsError() {
		return nil, checkError(resp)
	}
	return &result, nil
}

func (c *Client) RefreshDataTransformStatus(ctx context.Context, nameOrID string) (*types.DataCloudActionResponse, error) {
	var result types.DataCloudActionResponse
	resp, err := c.request(ctx).
		// EnableDebug().
		SetPathParam("nameOrID", nameOrID).
		SetResult(&result).
		Post("/services/data/{version}/ssot/data-transforms/{nameOrID}/actions/refresh-status")
	if err != nil {
		return nil, fmt.Errorf("refresh data transform status request failed: %w", err)
	}
	if resp.IsError() {
		return nil, checkError(resp)
	}
	return &result, nil
}
