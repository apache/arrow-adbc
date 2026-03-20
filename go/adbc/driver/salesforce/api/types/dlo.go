package types

// DataLakeObjectRequest is the request body for creating a DLO.
//
// The API has two field representations: `fields` and `dataLakeFieldInputRepresentations`.
// `fields` appears in responses with a `type` enum and `keyQualifierFieldName`.
// `dataLakeFieldInputRepresentations` appears in create requests with a `dataType` enum
// (partially overlapping variants). The exact distinction is unclear — pending Salesforce clarification.
type DataLakeObjectRequest struct {
	Name                       string            `json:"name"`
	Label                      string            `json:"label"`
	Category                   Category          `json:"category"`
	Fields                     []DataObjectField `json:"fields,omitempty"`
	FieldInputRepresentations  []DataLakeField   `json:"dataLakeFieldInputRepresentations,omitempty"`
	DataspaceInfo              []DataspaceInfo   `json:"dataspaceInfo,omitempty"`
	OrgUnitIdentifierFieldName string            `json:"orgUnitIdentifierFieldName,omitempty"`
	RecordModifiedFieldName    string            `json:"recordModifiedFieldName,omitempty"`
	EventDateTimeFieldName     string            `json:"eventDateTimeFieldName,omitempty"` // required for `Engagement` Category
}

type DataType string

const (
	DataTypeUnsupported DataType = "Unsupported"
	DataTypeBoolean     DataType = "Boolean"
	DataTypeCurrency    DataType = "Currency"
	DataTypeDate        DataType = "Date"
	DataTypeDateOnly    DataType = "DateOnly"
	DataTypeDateTime    DataType = "DateTime"
	DataTypeEmail       DataType = "Email"
	DataTypeNumber      DataType = "Number"
	DataTypePercent     DataType = "Percent"
	DataTypePhone       DataType = "Phone"
	DataTypeText        DataType = "Text"
	DataTypeUrl         DataType = "Url"
)

// DataObjectField
type DataObjectField struct {
	IsPrimaryKey      bool     `json:"isPrimaryKey"`
	DataType          DataType `json:"type"`
	Name              string   `json:"name"`
	Label             string   `json:"label"`
	KeyQualifierField string   `json:"keyQualifierFieldName,omitempty"`
}

// DataLakeField
type DataLakeField struct {
	IsPrimaryKey bool     `json:"isPrimaryKey"`
	DataType     DataType `json:"dataType"`
	Name         string   `json:"name"`
	Label        string   `json:"label"`
}

// DataLakeObjectCollection is the wrapper response from the GET endpoint.
type DataLakeObjectCollection struct {
	Paginated
	Items[DataLakeObject] `json:"dataLakeObjects"`
}

// DataLakeObject is the response representation of a DLO.
type DataLakeObject struct {
	ID                        string                `json:"id,omitempty"`
	Name                      string                `json:"name"`
	Category                  Category              `json:"category"`
	Status                    Status                `json:"status,omitempty"`
	Fields                    []DataObjectField     `json:"fields,omitempty"`
	FieldInputRepresentations []DataLakeFieldOutput `json:"dataLakeFieldInfoRepresentation,omitempty"`
}

func (dlo *DataLakeObject) IsActive() bool { return dlo.Status.IsActive() }
func (dlo *DataLakeObject) IsError() bool  { return dlo.Status.IsError() }

// DataLakeFieldOutput is a field in the DLO response.
type DataLakeFieldOutput struct {
	Name         string `json:"name"`
	DisplayName  string `json:"displayName"`
	Type         string `json:"type"`
	IsPrimaryKey bool   `json:"isPrimaryKey"`
}

type DataspaceInfo struct {
	Name string `json:"name"`
}
