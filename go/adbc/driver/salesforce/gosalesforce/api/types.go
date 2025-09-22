package api

import (
	"strings"
	"time"
)

// AuthType represents the type of authentication flow
type AuthType int

const (
	AuthTypeJWT AuthType = iota
	AuthTypeUsernamePassword
	AuthTypeRefreshToken
)

const DATASPACE_DEFAULT = "default"

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

// SfdcError represents an authentication-related error
type SfdcError struct {
	Code    int
	Message string
	Type    string
}

func (e *SfdcError) Error() string {
	return e.Message
}

// SqlQueryRequest represents a SQL query request to Data Cloud
type SqlQueryRequest struct {
	SQL           string         `json:"sql"`
	RowLimit      int64          `json:"rowLimit,omitempty"`
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
	Name      string  `json:"name"`
	Nullable  bool    `json:"nullable"`
	Type      SqlType `json:"type"`
	Precision *int    `json:"precision,omitempty"`
	Scale     *int    `json:"scale,omitempty"`
}

// SqlType represents the type of a SQL query result column
type SqlType string

const (
	SqlTypeArrayOfX    SqlType = "ArrayOfX"
	SqlTypeBigInt      SqlType = "BigInt"
	SqlTypeBool        SqlType = "Bool"
	SqlTypeChar        SqlType = "Char"
	SqlTypeDate        SqlType = "Date"
	SqlTypeDouble      SqlType = "Double"
	SqlTypeFloat       SqlType = "Float"
	SqlTypeInteger     SqlType = "Integer"
	SqlTypeNumeric     SqlType = "Numeric"
	SqlTypeOid         SqlType = "Oid"
	SqlTypeSmallInt    SqlType = "SmallInt"
	SqlTypeTime        SqlType = "Time"
	SqlTypeTimestamp   SqlType = "Timestamp"
	SqlTypeTimestampTZ SqlType = "TimestampTZ"
	SqlTypeUnspecified SqlType = "Unspecified"
	SqlTypeVarchar     SqlType = "Varchar"
)

// ToDataLakeFieldDataType converts a SqlType to a DataLakeFieldDataType
func (s SqlType) ToDataLakeFieldDataType() DataLakeFieldDataType {
	switch s {
	case SqlTypeBool:
		return DataLakeFieldDataTypeBoolean
	case SqlTypeDate:
		return DataLakeFieldDataTypeDate
	case SqlTypeTimestamp, SqlTypeTimestampTZ:
		return DataLakeFieldDataTypeDateTime
	case SqlTypeBigInt, SqlTypeInteger, SqlTypeSmallInt, SqlTypeDouble, SqlTypeFloat, SqlTypeNumeric:
		return DataLakeFieldDataTypeNumber
	case SqlTypeChar, SqlTypeVarchar, SqlTypeArrayOfX, SqlTypeOid, SqlTypeUnspecified:
		return DataLakeFieldDataTypeText
	case SqlTypeTime:
		// TODO: investigates if this is a proper conversion
		return DataLakeFieldDataTypeNumber
	default:
		// Default fallback
		// TODO: supports `SqlTypeArrayOfX` properly
		return DataLakeFieldDataTypeText
	}
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
	Name         string  `json:"name"`
	DisplayName  string  `json:"displayName"`
	Type         SqlType `json:"type"`
	KeyQualifier string  `json:"keyQualifier,omitempty"`
	BusinessType string  `json:"businessType,omitempty"`
	Precision    int     `json:"precision,omitempty"`
	Scale        int     `json:"scale,omitempty"`
	Nullable     bool    `json:"nullable,omitempty"`
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
	DataTransformTypeBatch DataTransformType = "BATCH"
	// "STREAMING" is not supported on purpose since it's not supposed to be used by dbt
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
	// This type is not publicly available yet; be cautious of possible breaking changes in the future.
	// This is only meant to use together with a DbtDataTransformDefinition (see DataTransformDefinition.Manifest)
	DataTransformDefinitionTypeDCSQL DataTransformDefinitionType = "DCSQL"
	// "SQL" and "STL" are not supported on purpose since they are not supposed to be used by dbt
	// "SQL" is sql expression that defines the streaming data transform
	// "STL" is the DSL used to describe the Node components that defines the batch data transform
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

// TODO: revisit these values, the actual returns are different from the documentation
// see LastRunStatus from https://developer.salesforce.com/docs/data/connectapi/references/spec?meta=getDataTransform
const (
	DataTransformLastRunStatusCanceled          DataTransformLastRunStatus = "Canceled"
	DataTransformLastRunStatusFailure           DataTransformLastRunStatus = "Failure"
	DataTransformLastRunStatusInProgress        DataTransformLastRunStatus = "In_Progress"
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
// This only supports BATCH data transform
type DataTransformDefinition struct {
	Type    DataTransformDefinitionType `json:"type"`
	Version string                      `json:"version"`
	// This feature is not publicly available yet; be cautious of possible breaking changes in the future.
	Manifest          DbtDataTransformDefinition      `json:"manifest,omitempty"`
	OutputDataObjects []DataTransformOutputDataObject `json:"outputDataObjects,omitempty"`
}

type DbtDataTransformDefinition struct {
	Nodes map[string]DbtDataTransformNode `json:"nodes,omitempty"`
}

// Information about the objects into which the data transform writes the transformed data.
// See `outputDataObjects` from reference: https://developer.salesforce.com/docs/data/connectapi/references/spec?meta=type:Batch+Data+Transform+Output
type DataTransformOutputDataObject struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace,omitempty"`
	// TODO: There are more fields known to be missing, add if necessary
}

// DataTransformNode represents a node in a data transform
type DbtDataTransformNode struct {
	// The name of the node, must match its associated key in the DbtDataTransformDefinition.Nodes map
	// Otherwise, it seems that you can arbitrarily choose a value and without impacting the results of the operation
	Name string `json:"name"`
	// The name of the target DLO/DMO of this data transform
	RelationName string                     `json:"relation_name,omitempty"`
	Config       DbtDataTransformNodeConfig `json:"config"`
	CompiledCode string                     `json:"compiled_code"`
	DependsOn    map[string]interface{}     `json:"depends_on,omitempty"`
}

type DbtDataTransformNodeConfig struct {
	Materialized string `json:"materialized"`
}

// DataTransform represents the response from creating a data transform
// see Responses from https://developer.salesforce.com/docs/data/connectapi/references/spec?meta=createDataTransform
type DataTransform struct {
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

type DataTransformList struct {
	DataTransforms []DataTransform `json:"dataTransforms"`
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
// reference: https://help.salesforce.com/s/articleView?id=data.c360_a_data_types.htm&type=5
type DataLakeFieldDataType string

const (
	DataLakeFieldDataTypeBoolean  DataLakeFieldDataType = "Boolean"
	DataLakeFieldDataTypeDate     DataLakeFieldDataType = "Date"
	DataLakeFieldDataTypeDateOnly DataLakeFieldDataType = "DateOnly"
	DataLakeFieldDataTypeDateTime DataLakeFieldDataType = "DateTime"
	DataLakeFieldDataTypeEmail    DataLakeFieldDataType = "Email"
	DataLakeFieldDataTypeNumber   DataLakeFieldDataType = "Number"
	DataLakeFieldDataTypePercent  DataLakeFieldDataType = "Percent"
	DataLakeFieldDataTypePhone    DataLakeFieldDataType = "Phone"
	DataLakeFieldDataTypeText     DataLakeFieldDataType = "Text"
	DataLakeFieldDataTypeUrl      DataLakeFieldDataType = "Url"
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
	Name     string                 `json:"name"`
	Label    string                 `json:"label"`
	Category DataLakeObjectCategory `json:"category"`
	// WARNING: `dataspaceName` field is seen in the doc but when passing it in, request fails.
	// Therefore, use `dataspaceInfo` here instead
	// This is recommended to set, otherwise it'll lead to a DLO unassigned to any data space which makes it not queryable via the SQL API
	DataspaceInfo                     []DataspaceInfo                    `json:"dataspaceInfo"`
	OrgUnitIdentifierFieldName        string                             `json:"orgUnitIdentifierFieldName"`
	RecordModifiedFieldName           string                             `json:"recordModifiedFieldName"`
	DataLakeFieldInputRepresentations []DataLakeFieldInputRepresentation `json:"dataLakeFieldInputRepresentations"`
	EventDateTimeFieldName            string                             `json:"eventDateTimeFieldName,omitempty"`
}

// DataspaceInfo represents information about a data space
type DataspaceInfo struct {
	Name string `json:"name"`
	// TODO: There are more fields known to be missing, add if necessary
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

type DataLakeObjects struct {
	DataLakeObjects []DataLakeObject `json:"dataLakeObjects"`
}

// DataLakeObject represents the response from creating a Data Lake Object
type DataLakeObject struct {
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

func (d *DataLakeObject) IsActive() bool {
	return strings.EqualFold(string(d.Status), string(DataLakeObjectStatusActive))
}

func (d *DataLakeObject) IsError() bool {
	return strings.EqualFold(string(d.Status), string(DataLakeObjectStatusError))
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

// Error response.
// reference: https://developer.salesforce.com/docs/data/connectapi/references/spec?meta=type:Data+Cloud+Error
type DataCloudError struct {
	ErrorCode    string `json:"errorCode"`
	ErrorMessage string `json:"errorMessage"`
}

// Data Cloud action response base.
// reference: https://developer.salesforce.com/docs/data/connectapi/references/spec?meta=type:Data+Cloud+Action+Response+Base
type DataCloudActionResponse struct {
	Errors  []DataCloudError `json:"errors"`
	Success bool             `json:"success"`
}

func (s *DataTransform) IsActive() bool {
	// Though it's documented that the status value is Camel case
	// see `status` from https://developer.salesforce.com/docs/data/connectapi/references/spec?meta=getDataTransform
	// it's actually returned as all upper case
	return strings.EqualFold(string(s.Status), string(DataTransformStatusActive))
}

func (s *DataTransform) IsError() bool {
	return strings.EqualFold(string(s.Status), string(DataTransformStatusError))
}

func (s *DataTransform) IsLastRunSuccess() bool {
	return strings.EqualFold(string(s.LastRunStatus), string(DataTransformLastRunStatusSuccess))
}

func (s *DataTransform) IsLastRunFailure() bool {
	return strings.EqualFold(string(s.LastRunStatus), string(DataTransformLastRunStatusFailure)) ||
		strings.EqualFold(string(s.LastRunStatus), string(DataTransformLastRunStatusPartialFailure))
}

func (s *DataTransform) IsLastRunCanceled() bool {
	return strings.EqualFold(string(s.LastRunStatus), string(DataTransformLastRunStatusCanceled)) ||
		strings.EqualFold(string(s.LastRunStatus), string(DataTransformLastRunStatusPartiallyCanceled))
}

func (s *DataTransform) IsLastRunInProgress() bool {
	return strings.EqualFold(string(s.LastRunStatus), string(DataTransformLastRunStatusInProgress))
}

func (s *DataTransform) IsLastRunPending() bool {
	return strings.EqualFold(string(s.LastRunStatus), string(DataTransformLastRunStatusPending))
}

// DataStreamType represents the type of data stream
type DataStreamType string

const (
	DataStreamTypeConnectorFramework DataStreamType = "ConnectorFramework"
	DataStreamTypeEvents             DataStreamType = "Events"
	DataStreamTypeExternal           DataStreamType = "External"
	DataStreamTypeIngestAPI          DataStreamType = "IngestAPI"
	DataStreamTypeMc                 DataStreamType = "Mc"
	DataStreamTypeMcde               DataStreamType = "Mcde"
	DataStreamTypeSfdc               DataStreamType = "Sfdc"
)

// DataStreamStatus represents the status of a data stream
type DataStreamStatus string

const (
	DataStreamStatusActive     DataStreamStatus = "Active"
	DataStreamStatusDeleting   DataStreamStatus = "Deleting"
	DataStreamStatusError      DataStreamStatus = "Error"
	DataStreamStatusProcessing DataStreamStatus = "Processing"
)

// DataStreamLastRunStatus represents the status of the last run
type DataStreamLastRunStatus string

const (
	DataStreamLastRunStatusCancelled  DataStreamLastRunStatus = "Cancelled"
	DataStreamLastRunStatusExtracting DataStreamLastRunStatus = "Extracting"
	DataStreamLastRunStatusFailure    DataStreamLastRunStatus = "Failure"
	DataStreamLastRunStatusInProgress DataStreamLastRunStatus = "In Progress"
	DataStreamLastRunStatusNone       DataStreamLastRunStatus = "None"
	DataStreamLastRunStatusPending    DataStreamLastRunStatus = "Pending"
	DataStreamLastRunStatusSuccess    DataStreamLastRunStatus = "Success"
)

// DataAccessMode represents the data access mode used to create the data stream
type DataAccessMode string

const (
	DataAccessModeDirectAccess DataAccessMode = "Direct_Access"
	DataAccessModeIngest       DataAccessMode = "Ingest"
)

// DataStreamFrequencyType represents how often the data stream is refreshed
type DataStreamFrequencyType string

const (
	DataStreamFrequencyTypeDaily           DataStreamFrequencyType = "Daily"
	DataStreamFrequencyTypeHourly          DataStreamFrequencyType = "Hourly"
	DataStreamFrequencyTypeMinutely        DataStreamFrequencyType = "Minutely"
	DataStreamFrequencyTypeMonthly         DataStreamFrequencyType = "Monthly"
	DataStreamFrequencyTypeMonthlyRelative DataStreamFrequencyType = "MonthlyRelative"
	DataStreamFrequencyTypeNone            DataStreamFrequencyType = "None"
	DataStreamFrequencyTypeWeekly          DataStreamFrequencyType = "Weekly"
)

// DataStreamRefreshDayOfWeek represents which day of the week the data stream is refreshed
type DataStreamRefreshDayOfWeek string

const (
	DataStreamRefreshDayOfWeekFriday    DataStreamRefreshDayOfWeek = "Friday"
	DataStreamRefreshDayOfWeekMonday    DataStreamRefreshDayOfWeek = "Monday"
	DataStreamRefreshDayOfWeekSaturday  DataStreamRefreshDayOfWeek = "Saturday"
	DataStreamRefreshDayOfWeekSunday    DataStreamRefreshDayOfWeek = "Sunday"
	DataStreamRefreshDayOfWeekThursday  DataStreamRefreshDayOfWeek = "Thursday"
	DataStreamRefreshDayOfWeekTuesday   DataStreamRefreshDayOfWeek = "Tuesday"
	DataStreamRefreshDayOfWeekWednesday DataStreamRefreshDayOfWeek = "Wednesday"
)

// DataStreamRefreshMode represents the refresh mode of the refresh configuration
type DataStreamRefreshMode string

const (
	DataStreamRefreshModeFullRefresh             DataStreamRefreshMode = "FullRefresh"
	DataStreamRefreshModeIncremental             DataStreamRefreshMode = "Incremental"
	DataStreamRefreshModeNearRealTimeIncremental DataStreamRefreshMode = "NearRealTimeIncremental"
	DataStreamRefreshModePartialUpdate           DataStreamRefreshMode = "PartialUpdate"
	DataStreamRefreshModeReplace                 DataStreamRefreshMode = "Replace"
	DataStreamRefreshModeUpsertMode              DataStreamRefreshMode = "UpsertMode"
)

// DataStreamConnectorDetails represents details about the Data Cloud connector
type DataStreamConnectorDetails struct {
	Name         string `json:"name"`
	SourceObject string `json:"sourceObject"`
}

// DataStreamConnectorInfo represents data stream connector information
type DataStreamConnectorInfo struct {
	ConnectorDetails DataStreamConnectorDetails `json:"connectorDetails"`
	ConnectorType    string                     `json:"connectorType"`
}

// DataStreamSourceField represents a source field in the data stream
type DataStreamSourceField struct {
	Datatype string `json:"datatype"`
	Format   string `json:"format,omitempty"`
	Name     string `json:"name"`
}

// DataStreamFieldMapping represents field mapping for the data stream
type DataStreamFieldMapping struct {
	Formula         string `json:"formula"`
	SourceFieldName string `json:"sourceFieldName"`
	TargetFieldName string `json:"targetFieldName"`
}

// DataStreamFrequency represents frequency of the refresh configuration
type DataStreamFrequency struct {
	FrequencyType     DataStreamFrequencyType     `json:"frequencyType"`
	Hours             []int                       `json:"hours"`
	RefreshDayOfMonth []int                       `json:"refreshDayOfMonth"`
	RefreshDayOfWeek  *DataStreamRefreshDayOfWeek `json:"refreshDayOfWeek,omitempty"`
}

// DataStreamRefreshConfig represents data stream refresh configuration
type DataStreamRefreshConfig struct {
	Frequency                         DataStreamFrequency   `json:"frequency"`
	HasHeaders                        bool                  `json:"hasHeaders"`
	IsAccelerationEnabled             *bool                 `json:"isAccelerationEnabled,omitempty"`
	RefreshMode                       DataStreamRefreshMode `json:"refreshMode"`
	ShouldFetchImmediately            bool                  `json:"shouldFetchImmediately"`
	ShouldTreatMissingFilesAsFailures bool                  `json:"shouldTreatMissingFilesAsFailures"`
}

// DataStream represents a data stream
type DataStream struct {
	// Inherited from Asset Base Output
	CreatedBy DataTransformUser `json:"createdBy"`
	ID        string            `json:"id"`
	Label     string            `json:"label"`
	Name      string            `json:"name"`
	URL       string            `json:"url,omitempty"`

	// Data stream specific fields
	AdvancedAttributes   map[string]interface{}   `json:"advancedAttributes,omitempty"`
	ConnectorInfo        DataStreamConnectorInfo  `json:"connectorInfo"`
	DataAccessMode       *DataAccessMode          `json:"dataAccessMode,omitempty"`
	DataLakeObjectInfo   DataLakeObject           `json:"dataLakeObjectInfo"`
	DataSource           string                   `json:"dataSource,omitempty"`
	DataStreamType       *DataStreamType          `json:"dataStreamType,omitempty"`
	IsEnabled            *bool                    `json:"isEnabled,omitempty"`
	LastAddedRecords     *int64                   `json:"lastAddedRecords,omitempty"`
	LastProcessedRecords *int64                   `json:"lastProcessedRecords,omitempty"`
	LastRefreshDate      string                   `json:"lastRefreshDate,omitempty"`
	LastRunStatus        *DataStreamLastRunStatus `json:"lastRunStatus,omitempty"`
	Mappings             []DataStreamFieldMapping `json:"mappings"`
	RecordID             string                   `json:"recordId,omitempty"`
	RefreshConfig        *DataStreamRefreshConfig `json:"refreshConfig,omitempty"`
	SourceFields         []DataStreamSourceField  `json:"sourceFields"`
	Status               *DataStreamStatus        `json:"status,omitempty"`
	TotalRecords         *int64                   `json:"totalRecords,omitempty"`
}
