package api

import (
	"context"
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc/driver/salesforce/api/types"
)

// UpsertDataSpaceMembers adds or updates members in a data space.
func (c *Client) UpsertDataSpaceMembers(ctx context.Context, dataSpace string, members []types.DataSpaceMember) (any, error) {
	type requestBody struct {
		Members struct {
			Members []types.DataSpaceMember `json:"members"`
		} `json:"members"`
	}

	type responseBody struct {
		// *types.DataCloudActionResponse
		Success bool  `json:"success,string"`
		Errors  []any `json:"errors"` // TODO: properly type this
		Members struct {
			Members []types.DataSpaceMember `json:"members"`
		} `json:"dataSpaceMembers"`
	}

	var reqBody requestBody
	reqBody.Members.Members = members

	var result responseBody
	resp, err := c.request(ctx).
		SetBody(&reqBody).
		SetResult(&result).
		SetPathParam("dataSpace", dataSpace).
		Put("/services/data/{version}/ssot/data-spaces/{dataSpace}/members")
	if err != nil {
		return nil, fmt.Errorf("upsert data space members request failed: %w", err)
	}
	if resp.IsError() {
		return nil, checkError(resp)
	}

	return result, nil
}
