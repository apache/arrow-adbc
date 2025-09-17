package api

import (
	"context"
	"fmt"
	"net/url"
	"slices"
	"strings"
)

// PostDataLakeObject creates a new Data Lake Object (DLO) in Data Cloud
// reference: https://developer.salesforce.com/docs/data/data-cloud-ref/guide/c360a-api-data-lake-objects.html
func (c *Client) PostDataLakeObject(ctx context.Context, request *CreateDataLakeObjectRequest) (*DataLakeObject, error) {
	if request == nil {
		return nil, &SfdcError{
			Code:    400,
			Message: "Data Lake Object request cannot be nil",
			Type:    "invalid_request",
		}
	}

	if request.Name == "" {
		return nil, &SfdcError{
			Code:    400,
			Message: "Data Lake Object name cannot be empty",
			Type:    "invalid_request",
		}
	}

	if request.Label == "" {
		return nil, &SfdcError{
			Code:    400,
			Message: "Data Lake Object label cannot be empty",
			Type:    "invalid_request",
		}
	}

	return PostJSON[CreateDataLakeObjectRequest, DataLakeObject](c, ctx, "data-lake-objects", request)
}

// GetDataLakeObject retrieves a specific Data Lake Object (DLO) by ID or developer name
// reference: https://developer.salesforce.com/docs/data/data-cloud-ref/guide/c360a-api-data-lake-objects.html
func (c *Client) GetDataLakeObject(ctx context.Context, recordIdOrDeveloperName string, limit, offset *int, orderBy string) (*DataLakeObject, error) {
	if recordIdOrDeveloperName == "" {
		return nil, &SfdcError{
			Code:    400,
			Message: "Record ID or developer name cannot be empty",
			Type:    "invalid_request",
		}
	}

	queryParams := url.Values{}
	if limit != nil {
		queryParams.Add("limit", fmt.Sprintf("%d", *limit))
	}
	if offset != nil {
		queryParams.Add("offset", fmt.Sprintf("%d", *offset))
	}
	if orderBy != "" {
		queryParams.Add("orderBy", orderBy)
	}
	return GetJSON[DataLakeObject](c, ctx, fmt.Sprintf("data-lake-objects/%s", recordIdOrDeveloperName), queryParams)
}

// DeleteDataLakeObject deletes a Data Lake Object (DLO) by ID or developer name
// reference: https://developer.salesforce.com/docs/data/data-cloud-ref/guide/c360a-api-data-lake-objects.html
func (c *Client) DeleteDataLakeObject(ctx context.Context, recordIdOrDeveloperName string) error {
	if recordIdOrDeveloperName == "" {
		return &SfdcError{
			Code:    400,
			Message: "Record ID or developer name cannot be empty",
			Type:    "invalid_request",
		}
	}
	return DeleteJSON(c, ctx, fmt.Sprintf("data-lake-objects/%s", recordIdOrDeveloperName))
}

// GetDataLakeObjectByID retrieves a DLO by its 18-character ID
func (c *Client) GetDataLakeObjectByID(ctx context.Context, id string) (*DataLakeObject, error) {
	return c.GetDataLakeObject(ctx, id, nil, nil, "")
}

// GetDataLakeObjectByName retrieves a DLO by its developer name
func (c *Client) GetDataLakeObjectByName(ctx context.Context, name string) (*DataLakeObject, error) {
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

// SqlMetadataToDataLakeFields converts SQL query metadata to Data Lake field representations
func SqlMetadataToDataLakeFields(metadata []SqlQueryMetadata, primaryKeyFieldName string) []DataLakeFieldInputRepresentation {
	fields := make([]DataLakeFieldInputRepresentation, 0, len(metadata))

	for _, meta := range metadata {
		isPrimaryKey := meta.Name == primaryKeyFieldName
		dataType := meta.Type.ToDataLakeFieldDataType()

		field := NewDataLakeField(
			meta.Name,
			meta.Name, // Using name as label, could be enhanced to use display name
			dataType,
			isPrimaryKey,
		)

		fields = append(fields, field)
	}

	return fields
}

// NewDataLakeObjectFromSqlResponse creates a Data Lake Object request from SQL query response metadata
func NewDataLakeObjectFromSqlResponse(name, label string, category DataLakeObjectCategory, sqlResponse *SqlQueryResponse, primaryKeyFieldName string) *CreateDataLakeObjectRequest {
	filteredMetadata := make([]SqlQueryMetadata, 0, len(sqlResponse.Metadata))

	// The filtered out fields are pseudo columns automatically filled by the Data Cloud
	// The postDataLakeObject request will fail if any of these fields are included in the request
	excludedFields := []string{"DataSourceObject__c", "InternalOrganization__c", "DataSource__c"}
	for _, meta := range sqlResponse.Metadata {
		if strings.HasPrefix(meta.Name, "cdp_sys_") || strings.HasPrefix(meta.Name, "KQ_") || slices.Contains(excludedFields, meta.Name) {
			continue
		} else {
			filteredMetadata = append(filteredMetadata, meta)
		}
	}
	fields := SqlMetadataToDataLakeFields(filteredMetadata, primaryKeyFieldName)
	return NewDataLakeObjectRequest(name, label, category, fields)
}
