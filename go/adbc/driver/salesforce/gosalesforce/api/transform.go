package api

import (
	"context"
	"fmt"
)

// CreateDataTransform creates a new data transform in Data Cloud
// reference: https://developer.salesforce.com/docs/data/connectapi/references/spec?meta=createDataTransform
func (c *Client) CreateDataTransform(ctx context.Context, request *CreateDataTransformRequest) (*DataTransform, error) {
	// Validate required fields
	if request.Name == "" {
		return nil, &SfdcError{
			Code:    400,
			Message: "Data transform name cannot be empty",
			Type:    "invalid_request",
		}
	}

	if request.Label == "" {
		return nil, &SfdcError{
			Code:    400,
			Message: "Data transform label cannot be empty",
			Type:    "invalid_request",
		}
	}

	return PostJSON[CreateDataTransformRequest, DataTransform](c, ctx, "data-transforms", request)
}

// CreateDataTransform creates a new data transform in Data Cloud
// reference: https://developer.salesforce.com/docs/data/connectapi/references/spec?meta=getDataTransform
func (c *Client) GetDataTransform(ctx context.Context, dataTransformNameOrId string) (*DataTransform, error) {
	// Validate required fields
	if dataTransformNameOrId == "" {
		return nil, &SfdcError{
			Code:    400,
			Message: "Data transform name or ID cannot be empty",
			Type:    "invalid_request",
		}
	}

	path := fmt.Sprintf("data-transforms/%s", dataTransformNameOrId)
	return GetJSON[DataTransform](c, ctx, path, nil)
}

// GetDataTransformByDLO returns all data transforms that are targeting a DLO of the input name
//
// TODO: also include data transforms that are sourcing from this DLO (may not be feasible from the existing API)
// or we also need to include the data space Name
//
// TODO: confirm if the DLO name is enough to uniquely identity a DLO,
func (c *Client) GetDataTransformByDLO(ctx context.Context, dloName string) ([]DataTransform, error) {
	// Validate required fields
	if dloName == "" {
		return nil, &SfdcError{
			Code:    400,
			Message: "Data transform name or ID cannot be empty",
			Type:    "invalid_request",
		}
	}

	dataTransforms, err := GetJSON[DataTransformList](c, ctx, "data-transforms", nil)
	if err != nil {
		return nil, err
	}

	foundDataTransforms := []DataTransform{}
	for _, dataTransform := range dataTransforms.DataTransforms {
		for _, outputDataObject := range dataTransform.Definition.OutputDataObjects {
			if outputDataObject.Name == dloName {
				foundDataTransforms = append(foundDataTransforms, dataTransform)
			}
		}
	}
	return foundDataTransforms, nil
}

// RefreshDataTransformStatus refreshes the status of a data transform
// reference: https://developer.salesforce.com/docs/data/connectapi/references/spec?meta=refreshDataTransformStatus
func (c *Client) RefreshDataTransformStatus(ctx context.Context, dataTransformNameOrId string) (*DataCloudActionResponse, error) {
	// Validate required fields
	if dataTransformNameOrId == "" {
		return nil, &SfdcError{
			Code:    400,
			Message: "Data transform name or ID cannot be empty",
			Type:    "invalid_request",
		}
	}

	path := fmt.Sprintf("data-transforms/%s/actions/refresh-status", dataTransformNameOrId)
	return PostJSON[interface{}, DataCloudActionResponse](c, ctx, path, nil)
}

// RunDataTransform runs a data transform
// reference: https://developer.salesforce.com/docs/data/connectapi/references/spec?meta=runDataTransform
func (c *Client) RunDataTransform(ctx context.Context, dataTransformNameOrId string) (*DataCloudActionResponse, error) {
	// Validate required fields
	if dataTransformNameOrId == "" {
		return nil, &SfdcError{
			Code:    400,
			Message: "Data transform name or ID cannot be empty",
			Type:    "invalid_request",
		}
	}

	path := fmt.Sprintf("data-transforms/%s/actions/run", dataTransformNameOrId)
	return PostJSON[interface{}, DataCloudActionResponse](c, ctx, path, nil)
}

// DeleteDataTransform deletes a data transform
// reference: https://developer.salesforce.com/docs/data/connectapi/references/spec?meta=deleteDataTransform
func (c *Client) DeleteDataTransform(ctx context.Context, dataTransformNameOrId string) error {
	// Validate required fields
	if dataTransformNameOrId == "" {
		return &SfdcError{
			Code:    400,
			Message: "Data transform name or ID cannot be empty",
			Type:    "invalid_request",
		}
	}

	path := fmt.Sprintf("data-transforms/%s", dataTransformNameOrId)
	return DeleteJSON(c, ctx, path)
}

// NewBatchDataTransformRequest creates a new batch data transform request with dbt-style definition
func NewBatchDataTransformRequest(name, label string, nodes map[string]DbtDataTransformNode) *CreateDataTransformRequest {
	return &CreateDataTransformRequest{
		Name:  name,
		Label: label,
		Type:  DataTransformTypeBatch,
		Definition: DataTransformDefinition{
			Type:    DataTransformDefinitionTypeDCSQL,
			Version: "1.0",
			Manifest: DbtDataTransformDefinition{
				Nodes: nodes,
			},
		},
	}
}

// NewDbtDataTransformNode creates a new dbt-style data transform node
func NewDbtDataTransformNode(name, relationName, compiledCode string, materialized string, dependsOn map[string]interface{}) DbtDataTransformNode {
	return DbtDataTransformNode{
		Name:         name,
		RelationName: relationName,
		Config: DbtDataTransformNodeConfig{
			Materialized: materialized,
		},
		CompiledCode: compiledCode,
		DependsOn:    dependsOn,
	}
}

// NewSimpleDbtNode creates a simple dbt node with minimal configuration
// Materialized is set to table
// DependsOn is set to empty map
func NewSimpleDbtDataTransformNode(name, relationName, sql string) DbtDataTransformNode {
	return DbtDataTransformNode{
		Name:         name,
		RelationName: relationName,
		Config: DbtDataTransformNodeConfig{
			Materialized: "table",
		},
		CompiledCode: sql,
		DependsOn:    make(map[string]interface{}),
	}
}
