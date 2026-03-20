package api

import (
	"context"
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc/driver/salesforce/api/types"
)

func (c *Client) GetMetadata(ctx context.Context, req *types.MetadataRequest) (*types.MetadataResponse, error) {
	r := c.request(ctx)
	if req.Dataspace != "" {
		r.SetQueryParam("dataspace", req.Dataspace)
	}
	if req.EntityCategory != "" {
		r.SetQueryParam("entityCategory", req.EntityCategory)
	}
	if req.EntityName != "" {
		r.SetQueryParam("entityName", req.EntityName)
	}
	if req.EntityType != "" {
		r.SetQueryParam("entityType", req.EntityType)
	}

	var result types.MetadataResponse
	resp, err := r.SetResult(&result).Get("/services/data/{version}/ssot/metadata")
	if err != nil {
		return nil, fmt.Errorf("metadata request failed: %w", err)
	}
	if resp.IsError() {
		return nil, checkError(resp)
	}

	return &result, nil
}
