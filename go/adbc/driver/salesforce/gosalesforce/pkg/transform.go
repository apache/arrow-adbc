package api

import (
	"context"
)

// CreateDataTransform is a convenience function to create a data transform using available tokens
func CreateDataTransform(ctx context.Context, client *Client, request *CreateDataTransformRequest) (*DataTransformResponse, error) {
	if client == nil {
		return nil, &AuthError{
			Code:    400,
			Message: "Client cannot be nil",
			Type:    "invalid_client",
		}
	}

	// Check if we have any valid token (try access token first for data transforms)
	token := client.accessToken
	if err := validateToken(token, "access token"); err != nil {
		// Fall back to CDP token
		token = client.cdpToken
		if err := validateToken(token, "CDP token"); err != nil {
			return nil, err
		}
	}

	return client.CreateDataTransform(ctx, request)
}

// NewBatchDataTransformRequest creates a new batch data transform request with STL definition
func NewBatchDataTransformRequest(name, label string, nodes map[string]DataTransformNode) *CreateDataTransformRequest {
	return &CreateDataTransformRequest{
		Name:  name,
		Label: label,
		Type:  DataTransformTypeBatch,
		Definition: DataTransformDefinition{
			Type:    DataTransformDefinitionTypeSTL,
			Version: "60.0",
			Nodes:   nodes,
		},
	}
}

// NewStreamingDataTransformRequest creates a new streaming data transform request with SQL definition
func NewStreamingDataTransformRequest(name, label, expression, targetDlo string) *CreateDataTransformRequest {
	return &CreateDataTransformRequest{
		Name:  name,
		Label: label,
		Type:  DataTransformTypeStreaming,
		Definition: DataTransformDefinition{
			Type:       DataTransformDefinitionTypeSQL,
			Version:    "60.0",
			Expression: expression,
			TargetDlo:  targetDlo,
		},
	}
}

// NewLoadDatasetNode creates a new load dataset node for batch transforms
func NewLoadDatasetNode(datasetName, datasetType string, fields []string) DataTransformNode {
	return DataTransformNode{
		Action: "load",
		Parameters: DataTransformNodeParameters{
			Dataset: &DataTransformDataset{
				Name: datasetName,
				Type: datasetType,
			},
			Fields: fields,
			SampleDetails: &DataTransformSampleDetails{
				SortBy: []interface{}{},
				Type:   "TopN",
			},
		},
		Sources: []string{},
	}
}

// NewOutputNode creates a new output node for batch transforms
func NewOutputNode(outputName, outputType string, fieldMappings []DataTransformFieldMapping, sources []string) DataTransformNode {
	return DataTransformNode{
		Action: "outputD360",
		Parameters: DataTransformNodeParameters{
			FieldsMappings: fieldMappings,
			Name:           outputName,
			Type:           outputType,
		},
		Sources: sources,
	}
}

// NewFieldMapping creates a new field mapping for output nodes
func NewFieldMapping(sourceField, targetField string) DataTransformFieldMapping {
	return DataTransformFieldMapping{
		SourceField: sourceField,
		TargetField: targetField,
	}
}
