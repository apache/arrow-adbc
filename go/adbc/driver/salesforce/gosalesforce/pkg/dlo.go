package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// PostDataLakeObject creates a new Data Lake Object (DLO) in Data Cloud
// reference: https://developer.salesforce.com/docs/data/data-cloud-ref/guide/c360a-api-data-lake-objects.html
func (c *Client) PostDataLakeObject(ctx context.Context, request *CreateDataLakeObjectRequest) (*DataLakeObjectResponse, error) {
	if request == nil {
		return nil, &AuthError{
			Code:    400,
			Message: "Data Lake Object request cannot be nil",
			Type:    "invalid_request",
		}
	}

	if request.Name == "" {
		return nil, &AuthError{
			Code:    400,
			Message: "Data Lake Object name cannot be empty",
			Type:    "invalid_request",
		}
	}

	if request.Label == "" {
		return nil, &AuthError{
			Code:    400,
			Message: "Data Lake Object label cannot be empty",
			Type:    "invalid_request",
		}
	}

	// Prepare the request body
	requestBody, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal data lake object request: %w", err)
	}

	dloURL := c.buildServicesURL(c.accessToken.InstanceURL, "data-lake-objects")

	req, err := http.NewRequestWithContext(ctx, "POST", dloURL, strings.NewReader(string(requestBody)))
	if err != nil {
		return nil, fmt.Errorf("failed to create data lake object request: %w", err)
	}

	setCommonHeaders(req, c.accessToken.AccessToken)

	// Execute request with retries
	resp, err := c.executeHTTPRequest(ctx, req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read data lake object creation response: %w", err)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return nil, handleErrorResponse(resp.StatusCode, body, "data_lake_object_creation_failed")
	}

	var dloResponse DataLakeObjectResponse
	if err := json.Unmarshal(body, &dloResponse); err != nil {
		return nil, fmt.Errorf("failed to parse data lake object creation response: %w", err)
	}

	return &dloResponse, nil
}

// GetDataLakeObject retrieves a specific Data Lake Object (DLO) by ID or developer name
// reference: https://developer.salesforce.com/docs/data/data-cloud-ref/guide/c360a-api-data-lake-objects.html
func (c *Client) GetDataLakeObject(ctx context.Context, recordIdOrDeveloperName string, limit, offset *int, orderBy string) (*DataLakeObjectResponse, error) {
	if recordIdOrDeveloperName == "" {
		return nil, &AuthError{
			Code:    400,
			Message: "Record ID or developer name cannot be empty",
			Type:    "invalid_request",
		}
	}

	// Build DLO retrieval URL
	dloURL := c.buildServicesURL(c.accessToken.InstanceURL, fmt.Sprintf("data-lake-objects/%s", recordIdOrDeveloperName))

	// Create the request
	req, err := http.NewRequestWithContext(ctx, "GET", dloURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create get data lake object request: %w", err)
	}

	// Add query parameters if provided
	query := req.URL.Query()
	if limit != nil {
		query.Add("limit", fmt.Sprintf("%d", *limit))
	}
	if offset != nil {
		query.Add("offset", fmt.Sprintf("%d", *offset))
	}
	if orderBy != "" {
		query.Add("orderBy", orderBy)
	}
	req.URL.RawQuery = query.Encode()

	// Set headers
	setCommonHeaders(req, c.accessToken.AccessToken)

	// Execute request with retries
	resp, err := c.executeHTTPRequest(ctx, req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read get data lake object response: %w", err)
	}

	// Check for success
	if resp.StatusCode != http.StatusOK {
		return nil, handleErrorResponse(resp.StatusCode, body, "get_data_lake_object_failed")
	}

	// Parse response
	var dloResponse DataLakeObjectResponse
	if err := json.Unmarshal(body, &dloResponse); err != nil {
		return nil, fmt.Errorf("failed to parse get data lake object response: %w", err)
	}

	return &dloResponse, nil
}

// DeleteDataLakeObject deletes a Data Lake Object (DLO) by ID or developer name
// reference: https://developer.salesforce.com/docs/data/data-cloud-ref/guide/c360a-api-data-lake-objects.html
func (c *Client) DeleteDataLakeObject(ctx context.Context, recordIdOrDeveloperName string) error {
	if recordIdOrDeveloperName == "" {
		return &AuthError{
			Code:    400,
			Message: "Record ID or developer name cannot be empty",
			Type:    "invalid_request",
		}
	}

	// Build DLO deletion URL
	dloURL := c.buildServicesURL(c.accessToken.InstanceURL, fmt.Sprintf("data-lake-objects/%s", recordIdOrDeveloperName))

	// Create the request
	req, err := http.NewRequestWithContext(ctx, "DELETE", dloURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create delete data lake object request: %w", err)
	}

	// Set headers
	setCommonHeaders(req, c.accessToken.AccessToken)

	// Execute request with retries
	resp, err := c.executeHTTPRequest(ctx, req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Read response body for error handling
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read delete data lake object response: %w", err)
	}

	// Check for success (200 OK or 204 No Content)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return handleErrorResponse(resp.StatusCode, body, "delete_data_lake_object_failed")
	}

	return nil
}

// GetDataLakeObjectByID retrieves a DLO by its 18-character ID
func (c *Client) GetDataLakeObjectByID(ctx context.Context, id string) (*DataLakeObjectResponse, error) {
	return c.GetDataLakeObject(ctx, id, nil, nil, "")
}

// GetDataLakeObjectByName retrieves a DLO by its developer name
func (c *Client) GetDataLakeObjectByName(ctx context.Context, name string) (*DataLakeObjectResponse, error) {
	return c.GetDataLakeObject(ctx, name, nil, nil, "")
}

// DeleteDataLakeObjectByID deletes a DLO by its 18-character ID
func (c *Client) DeleteDataLakeObjectByID(ctx context.Context, id string) error {
	return c.DeleteDataLakeObject(ctx, id)
}

// DeleteDataLakeObjectByName deletes a DLO by its developer name
func (c *Client) DeleteDataLakeObjectByName(ctx context.Context, name string) error {
	return c.DeleteDataLakeObject(ctx, name)
}

// NewDataLakeObjectRequest creates a new Data Lake Object request with basic fields
func NewDataLakeObjectRequest(name, label string, category DataLakeObjectCategory, fields []DataLakeFieldInputRepresentation) *CreateDataLakeObjectRequest {
	return &CreateDataLakeObjectRequest{
		Name:                              name,
		Label:                             label,
		Category:                          category,
		DataspaceInfo:                     []DataspaceInfo{},
		OrgUnitIdentifierFieldName:        "",
		RecordModifiedFieldName:           "",
		DataLakeFieldInputRepresentations: fields,
	}
}

// NewDataLakeField creates a new Data Lake field representation
func NewDataLakeField(name, label string, dataType DataLakeFieldDataType, isPrimaryKey bool) DataLakeFieldInputRepresentation {
	primaryKeyStr := "false"
	if isPrimaryKey {
		primaryKeyStr = "true"
	}

	return DataLakeFieldInputRepresentation{
		Name:         name,
		Label:        label,
		DataType:     dataType,
		IsPrimaryKey: primaryKeyStr,
	}
}

// NewDataspaceInfo creates a new dataspace info with filter configuration
func NewDataspaceInfo(name string, conditions []FilterCondition, operator ConjunctiveOperator) DataspaceInfo {
	return DataspaceInfo{
		Name: name,
		Filter: FilterConfig{
			ConjunctiveOperator: operator,
			Conditions: FilterConditions{
				Conditions: conditions,
			},
		},
	}
}

// NewFilterCondition creates a new filter condition
func NewFilterCondition(fieldName, filterValue, tableName string, operator FilterOperator) FilterCondition {
	return FilterCondition{
		FieldName:   fieldName,
		FilterValue: filterValue,
		Operator:    operator,
		TableName:   tableName,
	}
}

// NewProfileDataLakeObject creates a new Profile category Data Lake Object request
func NewProfileDataLakeObject(name, label string, fields []DataLakeFieldInputRepresentation) *CreateDataLakeObjectRequest {
	return NewDataLakeObjectRequest(name, label, DataLakeObjectCategoryProfile, fields)
}

// NewEngagementDataLakeObject creates a new Engagement category Data Lake Object request
func NewEngagementDataLakeObject(name, label, eventDateTimeFieldName string, fields []DataLakeFieldInputRepresentation) *CreateDataLakeObjectRequest {
	request := NewDataLakeObjectRequest(name, label, DataLakeObjectCategoryEngagement, fields)
	request.EventDateTimeFieldName = eventDateTimeFieldName
	return request
}
