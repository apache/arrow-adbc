package api

import (
	"context"
	"fmt"
	"net/url"
)

// GetDataStream retrieves a specific data stream by ID or developer name
// reference: https://developer.salesforce.com/docs/data/data-cloud-ref/guide/c360a-api-data-streams.html
func (c *Client) GetDataStream(ctx context.Context, recordIdOrDeveloperName string, includeMappings *bool) (*DataStream, error) {
	if recordIdOrDeveloperName == "" {
		return nil, &SfdcError{
			Code:    400,
			Message: "Record ID or developer name cannot be empty",
			Type:    "invalid_request",
		}
	}

	queryParams := url.Values{}
	if includeMappings != nil {
		if *includeMappings {
			queryParams.Add("includeMappings", "true")
		} else {
			queryParams.Add("includeMappings", "false")
		}
	}

	return GetJSON[DataStream](c, ctx, fmt.Sprintf("data-streams/%s", recordIdOrDeveloperName), queryParams)
}
