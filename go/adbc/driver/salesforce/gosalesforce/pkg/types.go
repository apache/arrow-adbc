package api

import (
	"time"
)

// AuthType represents the type of authentication flow
type AuthType int

const (
	AuthTypeJWT AuthType = iota
	AuthTypeUsernamePassword
	AuthTypeRefreshToken
)

// Token represents an authenticated token with expiry information
type Token struct {
	AccessToken  string
	RefreshToken string
	InstanceURL  string
	TokenType    string
	ExpiresAt    time.Time
}

// IsExpired checks if the token is expired
func (t *Token) IsExpired() bool {
	if t.ExpiresAt.IsZero() {
		return false
	}
	return time.Now().After(t.ExpiresAt)
}

// AuthConfig holds authentication configuration
type AuthConfig struct {
	LoginURL     string
	ClientID     string
	ClientSecret string
	Username     string
	Password     string
	PrivateKey   string // PEM-encoded private key for JWT
	RefreshToken string
	Timeout      time.Duration
	MaxRetries   int
}

// DefaultAuthConfig returns a default authentication configuration
func DefaultAuthConfig() *AuthConfig {
	return &AuthConfig{
		LoginURL:   "https://login.salesforce.com",
		Timeout:    30 * time.Second,
		MaxRetries: 3,
	}
}

// AuthError represents an authentication-related error
type AuthError struct {
	Code    int
	Message string
	Type    string
}

func (e *AuthError) Error() string {
	return e.Message
}

// Common authentication errors
var (
	ErrInvalidCredentials = &AuthError{Code: 400, Message: "Invalid credentials", Type: "invalid_credentials"}
	ErrTokenExpired       = &AuthError{Code: 401, Message: "Token expired", Type: "token_expired"}
	ErrInvalidGrant       = &AuthError{Code: 400, Message: "Invalid grant", Type: "invalid_grant"}
	ErrInsufficientScope  = &AuthError{Code: 403, Message: "Insufficient scope", Type: "insufficient_scope"}
)

// SqlQueryRequest represents a SQL query request to Data Cloud
type SqlQueryRequest struct {
	SQL           string         `json:"sql"`
	RowLimit      *int64         `json:"rowLimit,omitempty"`
	SqlParameters []SqlParameter `json:"sqlParameters,omitempty"`
	Dataspace     string         `json:"dataspace,omitempty"`
	WorkloadName  string         `json:"workloadName,omitempty"`
}

// SqlParameter represents a parameter in a SQL query
type SqlParameter struct {
	Type  string      `json:"type"`
	Name  string      `json:"name"`
	Value interface{} `json:"value"`
}

// SqlQueryResponse represents the response from a SQL query
type SqlQueryResponse struct {
	Data         [][]interface{}    `json:"data"`
	Metadata     []SqlQueryMetadata `json:"metadata"`
	Status       SqlQueryStatus     `json:"status"`
	ReturnedRows int64              `json:"returnedRows"`
}

// SqlQueryMetadata represents metadata for a SQL query result column
type SqlQueryMetadata struct {
	Name      string `json:"name"`
	Nullable  bool   `json:"nullable"`
	Type      string `json:"type"`
	Precision *int   `json:"precision,omitempty"`
	Scale     *int   `json:"scale,omitempty"`
}

// SqlQueryStatus represents the status of a SQL query execution
type SqlQueryStatus struct {
	ChunkCount       int     `json:"chunkCount"`
	CompletionStatus string  `json:"completionStatus"`
	ExpirationTime   string  `json:"expirationTime"`
	Progress         float64 `json:"progress"`
	QueryId          string  `json:"queryId"`
	RowCount         int64   `json:"rowCount"`
}

// QueryV2Request represents a SQL query request to the v2 query API
type QueryV2Request struct {
	Sql string `json:"sql"`
}

// QueryV2Response represents the response from the v2 query API
// reference: https://developer.salesforce.com/docs/data/data-cloud-query-guide/references/data-cloud-query-api-reference/c360a-api-query-v2.html
type QueryV2Response struct {
	Data        [][]interface{}            `json:"data,omitempty"`
	Metadata    map[string]QueryV2Metadata `json:"metadata,omitempty"`
	Done        bool                       `json:"done"`
	NextBatchId *string                    `json:"nextBatchId,omitempty"`
	RowCount    int64                      `json:"rowCount,omitempty"`
	QueryId     string                     `json:"queryId,omitempty"`
	StartTime   string                     `json:"startTime,omitempty"`
	EndTime     string                     `json:"endTime,omitempty"`
	ArrowStream interface{}                `json:"arrowStream,omitempty"`
}

// QueryV2Metadata represents metadata for a v2 query result column
type QueryV2Metadata struct {
	Type         string `json:"type"`
	PlaceInOrder int    `json:"placeInOrder"`
	TypeCode     int    `json:"typeCode"`
}

// MetadataResponse represents the response from the metadata API
type MetadataResponse struct {
	Metadata []MetadataEntity `json:"metadata"`
}

// MetadataEntity represents a single metadata entity (table/object)
type MetadataEntity struct {
	Name                              string                 `json:"name"`
	DisplayName                       string                 `json:"displayName"`
	Category                          string                 `json:"category,omitempty"`
	Fields                            []MetadataField        `json:"fields,omitempty"`
	Indexes                           []interface{}          `json:"indexes,omitempty"`
	Relationships                     []MetadataRelationship `json:"relationships,omitempty"`
	PrimaryKeys                       []MetadataPrimaryKey   `json:"primaryKeys,omitempty"`
	ReferenceModelEntityDeveloperName string                 `json:"referenceModelEntityDeveloperName,omitempty"`
	Dimensions                        []MetadataDimension    `json:"dimensions,omitempty"`
	Measures                          []MetadataMeasure      `json:"measures,omitempty"`
	PartitionBy                       string                 `json:"partitionBy,omitempty"`
	LatestProcessTime                 string                 `json:"latestProcessTime,omitempty"`
	LatestSuccessfulProcessTime       string                 `json:"latestSuccessfulProcessTime,omitempty"`
}

// MetadataField represents a field/column in a metadata entity
type MetadataField struct {
	Name         string `json:"name"`
	DisplayName  string `json:"displayName"`
	Type         string `json:"type"`
	KeyQualifier string `json:"keyQualifier,omitempty"`
	BusinessType string `json:"businessType,omitempty"`
	Precision    int    `json:"precision,omitempty"`
	Scale        int    `json:"scale,omitempty"`
	Nullable     bool   `json:"nullable,omitempty"`
}

// MetadataRelationship represents a relationship between entities
type MetadataRelationship struct {
	FromEntity          string `json:"fromEntity"`
	ToEntity            string `json:"toEntity"`
	FromEntityAttribute string `json:"fromEntityAttribute,omitempty"`
	ToEntityAttribute   string `json:"toEntityAttribute,omitempty"`
	Cardinality         string `json:"cardinality,omitempty"`
}

// MetadataPrimaryKey represents a primary key field
type MetadataPrimaryKey struct {
	Name        string `json:"name"`
	DisplayName string `json:"displayName"`
	IndexOrder  string `json:"indexOrder"`
}

// MetadataDimension represents a dimension in a calculated insight
type MetadataDimension struct {
	Name         string `json:"name"`
	DisplayName  string `json:"displayName"`
	Type         string `json:"type"`
	BusinessType string `json:"businessType"`
}

// MetadataMeasure represents a measure in a calculated insight
type MetadataMeasure struct {
	Name         string `json:"name"`
	DisplayName  string `json:"displayName"`
	Type         string `json:"type"`
	Rollupable   bool   `json:"rollupable"`
	BusinessType string `json:"businessType"`
}

// CreateJobRequest represents a request to create a data ingestion job
type CreateJobRequest struct {
	Object     string `json:"object"`
	SourceName string `json:"sourceName"`
	Operation  string `json:"operation"` // "upsert" or "delete"
}

// CreateJobResponse represents the response from creating a job
type CreateJobResponse struct {
	ID          string `json:"id"`
	State       string `json:"state"`
	Object      string `json:"object"`
	Operation   string `json:"operation"`
	SourceName  string `json:"sourceName"`
	ContentType string `json:"contentType"`
	ContentURL  string `json:"contentUrl"`
}

// CloseJobRequest represents a request to close or abort a job
type CloseJobRequest struct {
	State string `json:"state"` // "UploadComplete" to close, "Aborted" to abort
}

// CloseJobResponse represents the response from closing or aborting a job
type CloseJobResponse struct {
	ID             string `json:"id"`
	Operation      string `json:"operation"`
	Object         string `json:"object"`
	CreatedById    string `json:"createdById"`
	CreatedDate    string `json:"createdDate"`
	SystemModstamp string `json:"systemModstamp"`
	State          string `json:"state"`
	ContentType    string `json:"contentType"`
	APIVersion     string `json:"apiVersion"`
}

// DataTransformType represents the type of data transform
type DataTransformType string

const (
	DataTransformTypeBatch     DataTransformType = "BATCH"
	DataTransformTypeStreaming DataTransformType = "STREAMING"
)

// DataTransformCreationType represents the creation type of the data transform
type DataTransformCreationType string

const (
	DataTransformCreationTypeCustom DataTransformCreationType = "Custom"
	DataTransformCreationTypeSystem DataTransformCreationType = "System"
)

// DataTransformDefinitionType represents the type of definition
type DataTransformDefinitionType string

const (
	DataTransformDefinitionTypeSQL       DataTransformDefinitionType = "SQL"
	DataTransformDefinitionTypeSTL       DataTransformDefinitionType = "STL"
	DataTransformDefinitionTypeDBT       DataTransformDefinitionType = "DBT"
	DataTransformDefinitionTypeSQLHidden DataTransformDefinitionType = "SqlHidden"
	DataTransformDefinitionTypeSTLHidden DataTransformDefinitionType = "StlHidden"
	DataTransformDefinitionTypeDBTHidden DataTransformDefinitionType = "DbtHidden"
)

// DataTransformStatus represents the status of the data transform
type DataTransformStatus string

const (
	DataTransformStatusActive     DataTransformStatus = "Active"
	DataTransformStatusDeleting   DataTransformStatus = "Deleting"
	DataTransformStatusError      DataTransformStatus = "Error"
	DataTransformStatusProcessing DataTransformStatus = "Processing"
)

// DataTransformLastRunStatus represents the status of the last run
type DataTransformLastRunStatus string

const (
	DataTransformLastRunStatusCanceled          DataTransformLastRunStatus = "Canceled"
	DataTransformLastRunStatusFailure           DataTransformLastRunStatus = "Failure"
	DataTransformLastRunStatusInProgress        DataTransformLastRunStatus = "InProgress"
	DataTransformLastRunStatusNone              DataTransformLastRunStatus = "None"
	DataTransformLastRunStatusPartialFailure    DataTransformLastRunStatus = "PartialFailure"
	DataTransformLastRunStatusPartiallyCanceled DataTransformLastRunStatus = "PartiallyCanceled"
	DataTransformLastRunStatusPending           DataTransformLastRunStatus = "Pending"
	DataTransformLastRunStatusSuccess           DataTransformLastRunStatus = "Success"
)

// CreateDataTransformRequest represents a request to create a data transform
type CreateDataTransformRequest struct {
	CreationType    DataTransformCreationType `json:"creationType,omitempty"`
	CurrencyIsoCode string                    `json:"currencyIsoCode,omitempty"`
	DataSpaceName   string                    `json:"dataSpaceName,omitempty"`
	Definition      DataTransformDefinition   `json:"definition"`
	Description     string                    `json:"description,omitempty"`
	Label           string                    `json:"label"`
	Name            string                    `json:"name"`
	PrimarySource   string                    `json:"primarySource,omitempty"`
	Type            DataTransformType         `json:"type"`
}

// DataTransformDefinition represents the base definition of a data transform
type DataTransformDefinition struct {
	Type    DataTransformDefinitionType `json:"type"`
	Version string                      `json:"version"`
	// For batch transforms
	Nodes map[string]DataTransformNode `json:"nodes,omitempty"`
	UI    interface{}                  `json:"ui,omitempty"`
	// For streaming transforms
	Expression        string                          `json:"expression,omitempty"`
	TargetDlo         string                          `json:"targetDlo,omitempty"`
	OutputDataObjects []DataTransformOutputDataObject `json:"outputDataObjects,omitempty"`
}

// DataTransformNode represents a node in a data transform
type DataTransformNode struct {
	Action     string                      `json:"action"`
	Parameters DataTransformNodeParameters `json:"parameters"`
	Sources    []string                    `json:"sources"`
}

// DataTransformNodeParameters represents parameters for a transform node
type DataTransformNodeParameters struct {
	// For load action
	Dataset       *DataTransformDataset       `json:"dataset,omitempty"`
	Fields        []string                    `json:"fields,omitempty"`
	SampleDetails *DataTransformSampleDetails `json:"sampleDetails,omitempty"`
	// For output action
	FieldsMappings []DataTransformFieldMapping `json:"fieldsMappings,omitempty"`
	Name           string                      `json:"name,omitempty"`
	Type           string                      `json:"type,omitempty"`
}

// DataTransformDataset represents a dataset reference
type DataTransformDataset struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// DataTransformSampleDetails represents sampling details
type DataTransformSampleDetails struct {
	SortBy []interface{} `json:"sortBy"`
	Type   string        `json:"type"`
}

// DataTransformFieldMapping represents field mapping for output
type DataTransformFieldMapping struct {
	SourceField string `json:"sourceField"`
	TargetField string `json:"targetField"`
}

// DataTransformOutputDataObject represents output data object information
type DataTransformOutputDataObject struct {
	Category         string               `json:"category,omitempty"`
	CreatedDate      string               `json:"createdDate,omitempty"`
	Fields           []DataTransformField `json:"fields,omitempty"`
	ID               string               `json:"id,omitempty"`
	Label            string               `json:"label"`
	LastModifiedDate string               `json:"lastModifiedDate,omitempty"`
	Name             string               `json:"name"`
	Status           string               `json:"status,omitempty"`
	Type             string               `json:"type"`
}

// DataTransformField represents a field in a data transform output
type DataTransformField struct {
	IsPrimaryKey      bool   `json:"isPrimaryKey,omitempty"`
	KeyQualifierField string `json:"keyQualifierField,omitempty"`
	Label             string `json:"label"`
	Name              string `json:"name"`
	Type              string `json:"type"`
}

// DataTransformResponse represents the response from creating a data transform
type DataTransformResponse struct {
	ActionUrls       DataTransformActionUrls    `json:"actionUrls,omitempty"`
	CreatedBy        DataTransformUser          `json:"createdBy"`
	CreatedDate      string                     `json:"createdDate"`
	CreationType     DataTransformCreationType  `json:"creationType,omitempty"`
	Definition       DataTransformDefinition    `json:"definition"`
	ID               string                     `json:"id"`
	Label            string                     `json:"label"`
	LastModifiedBy   DataTransformUser          `json:"lastModifiedBy"`
	LastModifiedDate string                     `json:"lastModifiedDate"`
	LastRunStatus    DataTransformLastRunStatus `json:"lastRunStatus"`
	Name             string                     `json:"name"`
	Namespace        string                     `json:"namespace,omitempty"`
	Status           DataTransformStatus        `json:"status"`
	Type             DataTransformType          `json:"type"`
	URL              string                     `json:"url"`
	DataSpaceName    string                     `json:"dataSpaceName,omitempty"`
	Description      string                     `json:"description,omitempty"`
	LastRunDate      string                     `json:"lastRunDate,omitempty"`
	Version          int64                      `json:"version,omitempty"`
}

// DataTransformActionUrls represents available actions for the data transform
type DataTransformActionUrls struct {
	CancelAction        string `json:"cancelAction,omitempty"`
	RefreshStatusAction string `json:"refreshStatusAction,omitempty"`
	RetryAction         string `json:"retryAction,omitempty"`
	RunAction           string `json:"runAction,omitempty"`
}

// DataTransformUser represents user information in data transform responses
type DataTransformUser struct {
	ID              string `json:"id"`
	Name            string `json:"name"`
	ProfilePhotoURL string `json:"profilePhotoUrl,omitempty"`
}

// DataLakeObjectCategory represents the category of a Data Lake Object
type DataLakeObjectCategory string

const (
	DataLakeObjectCategoryDirectoryTable DataLakeObjectCategory = "Directory_Table"
	DataLakeObjectCategoryEngagement     DataLakeObjectCategory = "Engagement"
	DataLakeObjectCategoryInsights       DataLakeObjectCategory = "Insights"
	DataLakeObjectCategoryOther          DataLakeObjectCategory = "Other"
	DataLakeObjectCategoryProfile        DataLakeObjectCategory = "Profile"
)

// DataLakeObjectStatus represents the status of a Data Lake Object
type DataLakeObjectStatus string

const (
	DataLakeObjectStatusActive     DataLakeObjectStatus = "Active"
	DataLakeObjectStatusDeleting   DataLakeObjectStatus = "Deleting"
	DataLakeObjectStatusError      DataLakeObjectStatus = "Error"
	DataLakeObjectStatusInactive   DataLakeObjectStatus = "Inactive"
	DataLakeObjectStatusProcessing DataLakeObjectStatus = "Processing"
)

// DataLakeFieldDataType represents the data type of a field
type DataLakeFieldDataType string

const (
	DataLakeFieldDataTypeText     DataLakeFieldDataType = "Text"
	DataLakeFieldDataTypeNumber   DataLakeFieldDataType = "Number"
	DataLakeFieldDataTypeDateTime DataLakeFieldDataType = "DateTime"
	DataLakeFieldDataTypeDate     DataLakeFieldDataType = "Date"
	DataLakeFieldDataTypeBoolean  DataLakeFieldDataType = "Boolean"
)

// FilterOperator represents filter operators
type FilterOperator string

const (
	FilterOperatorEquals FilterOperator = "EqualsOperator"
)

// ConjunctiveOperator represents conjunctive operators
type ConjunctiveOperator string

const (
	ConjunctiveOperatorAnd ConjunctiveOperator = "AndOperator"
	ConjunctiveOperatorOr  ConjunctiveOperator = "OrOperator"
)

// CreateDataLakeObjectRequest represents a request to create a Data Lake Object
type CreateDataLakeObjectRequest struct {
	Name                              string                             `json:"name"`
	Label                             string                             `json:"label"`
	Category                          DataLakeObjectCategory             `json:"category"`
	DataspaceInfo                     []DataspaceInfo                    `json:"dataspaceInfo"`
	OrgUnitIdentifierFieldName        string                             `json:"orgUnitIdentifierFieldName"`
	RecordModifiedFieldName           string                             `json:"recordModifiedFieldName"`
	DataLakeFieldInputRepresentations []DataLakeFieldInputRepresentation `json:"dataLakeFieldInputRepresentations"`
	EventDateTimeFieldName            string                             `json:"eventDateTimeFieldName,omitempty"`
}

// DataspaceInfo represents information about a data space
type DataspaceInfo struct {
	Name   string       `json:"name"`
	Filter FilterConfig `json:"filter"`
}

// FilterConfig represents filter configuration
type FilterConfig struct {
	ConjunctiveOperator ConjunctiveOperator `json:"conjunctiveOperator"`
	Conditions          FilterConditions    `json:"conditions"`
}

// FilterConditions represents filter conditions
type FilterConditions struct {
	Conditions []FilterCondition `json:"conditions"`
}

// FilterCondition represents a single filter condition
type FilterCondition struct {
	FieldName   string         `json:"fieldName"`
	FilterValue string         `json:"filterValue"`
	Operator    FilterOperator `json:"operator"`
	TableName   string         `json:"tableName"`
}

// DataLakeFieldInputRepresentation represents a field in the DLO input
type DataLakeFieldInputRepresentation struct {
	Name         string                `json:"name"`
	Label        string                `json:"label"`
	DataType     DataLakeFieldDataType `json:"dataType"`
	IsPrimaryKey string                `json:"isPrimaryKey"` // "true" or "false" as string
}

// DataLakeObjectResponse represents the response from creating a Data Lake Object
type DataLakeObjectResponse struct {
	Capabilities                    map[string]interface{} `json:"capabilities"`
	Category                        DataLakeObjectCategory `json:"category"`
	DataLakeFieldInfoRepresentation []DataLakeFieldOutput  `json:"dataLakeFieldInfoRepresentation"`
	DataSpaceInfo                   []DataSpaceObject      `json:"dataSpaceInfo"`
	Fields                          []DataLakeFieldOutput  `json:"fields"`
	ID                              string                 `json:"id"`
	Label                           string                 `json:"label"`
	Name                            string                 `json:"name"`
	Namespace                       string                 `json:"namespace"`
	Status                          DataLakeObjectStatus   `json:"status"`
	EventDateTimeFieldName          string                 `json:"eventDateTimeFieldName,omitempty"`
	OrgUnitIdentifierFieldName      string                 `json:"orgUnitIdentifierFieldName,omitempty"`
	RecordModifiedFieldName         string                 `json:"recordModifiedFieldName,omitempty"`
	CreatedBy                       DataTransformUser      `json:"createdBy,omitempty"`
	CreatedDate                     string                 `json:"createdDate,omitempty"`
	LastModifiedBy                  DataTransformUser      `json:"lastModifiedBy,omitempty"`
	LastModifiedDate                string                 `json:"lastModifiedDate,omitempty"`
	URL                             string                 `json:"url,omitempty"`
}

// DataLakeFieldOutput represents a field in the DLO output
type DataLakeFieldOutput struct {
	DataType     DataLakeFieldDataType `json:"dataType"`
	IsPrimaryKey bool                  `json:"isPrimaryKey"`
	Label        string                `json:"label"`
	Name         string                `json:"name"`
}

// DataSpaceObject represents data space information in the response
type DataSpaceObject struct {
	Filter FilterConfig `json:"filter"`
	Label  string       `json:"label"`
	Name   string       `json:"name"`
}
