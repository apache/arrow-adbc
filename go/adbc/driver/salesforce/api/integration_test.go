package api

import (
	"context"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc/driver/salesforce/api/types"
	"github.com/stretchr/testify/suite"
)

// IntegrationSuite runs integration tests against live Salesforce.
// Uses VCR cassettes for recording/replay via the embedded APISuite.
// Skipped in short mode: go test -short
type IntegrationSuite struct {
	APISuite
}

func (s *IntegrationSuite) SetupSuite() {
	if testing.Short() {
		s.T().Skip("skipping integration tests in short mode")
	}
}

// --- Auth + Basic Connectivity ---

func (s *IntegrationSuite) TestAuthAndBasicQuery() {
	ctx := context.Background()

	// Verify metadata endpoint works
	meta, err := s.Client.GetMetadata(ctx, &types.MetadataRequest{})
	s.Require().NoError(err)
	s.NotEmpty(meta.Metadata, "expected at least one metadata entity")
	s.T().Logf("Authenticated, %d entities available", len(meta.Metadata))

	// Run a simple query against the first entity
	entity := meta.Metadata[0]
	resp, err := s.Client.CreateSqlQuery(ctx, &types.SqlQueryRequest{
		SQL:      "SELECT * FROM " + entity.Name + " LIMIT 1",
		RowLimit: 1,
	}, nil)
	s.Require().NoError(err)
	s.NotEmpty(resp.Metadata, "expected column metadata")
	s.T().Logf("Query on %s: %d rows, %d columns", entity.Name, resp.ReturnedRows, len(resp.Metadata))
}

// --- Metadata ---

func (s *IntegrationSuite) TestMetadataEntities() {
	ctx := context.Background()

	meta, err := s.Client.GetMetadata(ctx, &types.MetadataRequest{})
	s.Require().NoError(err)
	s.NotEmpty(meta.Metadata)

	// Verify entity structure
	entity := meta.Metadata[0]
	s.NotEmpty(entity.Name)
	s.NotEmpty(entity.Fields, "expected fields on first entity")

	// Inspect first field
	field := entity.Fields[0]
	s.NotEmpty(field.Name)
	s.NotEmpty(field.Type)

	s.T().Logf("Entity %s: %d fields", entity.Name, len(entity.Fields))
	for i, f := range entity.Fields {
		if i >= 5 {
			s.T().Logf("  ... and %d more fields", len(entity.Fields)-5)
			break
		}
		nullable := "NOT NULL"
		if f.Nullable {
			nullable = "NULLABLE"
		}
		s.T().Logf("  - %s (%s) %s", f.Name, f.Type, nullable)
	}
}

func (s *IntegrationSuite) TestMetadataFilterByCategory() {
	ctx := context.Background()

	resp, err := s.Client.GetMetadata(ctx, &types.MetadataRequest{
		EntityCategory: "Profile",
	})
	s.Require().NoError(err)
	s.T().Logf("Profile entities: %d", len(resp.Metadata))
}

// --- SQL Query ---

func (s *IntegrationSuite) TestSqlQueryWithRowLimit() {
	ctx := context.Background()

	meta, err := s.Client.GetMetadata(ctx, &types.MetadataRequest{})
	s.Require().NoError(err)
	s.Require().NotEmpty(meta.Metadata)

	entity := meta.Metadata[0]
	resp, err := s.Client.CreateSqlQuery(ctx, &types.SqlQueryRequest{
		SQL:      "SELECT * FROM " + entity.Name + " LIMIT 5",
		RowLimit: 5,
	}, nil)
	s.Require().NoError(err)
	s.LessOrEqual(resp.ReturnedRows, int64(5))
	s.NotEmpty(resp.Metadata)

	// Verify column metadata has types
	for _, col := range resp.Metadata {
		s.NotEmpty(col.Name)
		s.NotEmpty(col.Type)
	}

	s.T().Logf("Query returned %d rows, %d columns", resp.ReturnedRows, len(resp.Metadata))
}

func (s *IntegrationSuite) TestSqlQueryStatus() {
	ctx := context.Background()

	meta, err := s.Client.GetMetadata(ctx, &types.MetadataRequest{})
	s.Require().NoError(err)
	s.Require().NotEmpty(meta.Metadata)

	entity := meta.Metadata[0]
	resp, err := s.Client.CreateSqlQuery(ctx, &types.SqlQueryRequest{
		SQL:      "SELECT * FROM " + entity.Name + " LIMIT 1",
		RowLimit: 1,
	}, nil)
	s.Require().NoError(err)
	s.NotEmpty(resp.Status.QueryID, "expected a query ID")
	s.T().Logf("Query status: %s, progress: %.1f, queryId: %s",
		resp.Status.CompletionStatus, resp.Status.Progress, resp.Status.QueryID)

	// Get the query status by ID
	status, err := s.Client.GetSqlQueryStatus(ctx, resp.Status.QueryID, 0, nil)
	s.Require().NoError(err)
	s.Equal(resp.Status.QueryID, status.QueryID)
	s.T().Logf("GetQuery status: %s, rowCount: %d", status.CompletionStatus, status.RowCount)
}

func (s *IntegrationSuite) TestGetQueryRows() {
	ctx := context.Background()

	meta, err := s.Client.GetMetadata(ctx, &types.MetadataRequest{})
	s.Require().NoError(err)
	s.Require().NotEmpty(meta.Metadata)

	entity := meta.Metadata[0]
	resp, err := s.Client.CreateSqlQuery(ctx, &types.SqlQueryRequest{
		SQL:      "SELECT * FROM " + entity.Name + " LIMIT 3",
		RowLimit: 3,
	}, nil)
	s.Require().NoError(err)
	s.NotEmpty(resp.Status.QueryID)

	// Fetch rows separately
	rows, err := s.Client.GetSqlQueryRows(ctx, resp.Status.QueryID, 0, 3, nil)
	s.Require().NoError(err)
	s.NotNil(rows)
	s.T().Logf("GetQueryRows: %d rows returned", rows.ReturnedRows)
}

// --- DLO ---

func (s *IntegrationSuite) TestCreateProfileDLO() {
	ctx := context.Background()
	name := "sftest_profile__dll"

	_ = s.Client.DeleteDataLakeObject(ctx, name)

	req := &types.DataLakeObjectRequest{
		Name:     name,
		Label:    "sftest Profile DLO",
		Category: types.CategoryProfile,
		FieldInputRepresentations: []types.DataLakeField{
			{Name: "id__c", Label: "ID", DataType: "Text", IsPrimaryKey: true},
			{Name: "first_name__c", Label: "First Name", DataType: "Text", IsPrimaryKey: false},
			{Name: "email__c", Label: "Email", DataType: "Email", IsPrimaryKey: false},
			{Name: "is_active__c", Label: "Is Active", DataType: "Boolean", IsPrimaryKey: false},
			{Name: "total_spent__c", Label: "Total Spent", DataType: "Number", IsPrimaryKey: false},
		},
	}

	dlo, err := s.Client.CreateDataLakeObject(ctx, req)
	if err != nil {
		sfErr, ok := err.(*SalesforceError)
		if ok && sfErr.Code == "INVALID_INPUT" {
			s.T().Skipf("DLO still exists from previous run: %v", err)
		}
		s.Require().NoError(err)
	}
	s.T().Logf("Created Profile DLO: %s (id=%s)", dlo.Name, dlo.ID)

	// Verify we can fetch it
	fetched, err := s.Client.GetDataLakeObject(ctx, name)
	s.Require().NoError(err)
	s.Equal(name, fetched.Name)

	// Cleanup (may fail if still processing)
	_ = s.Client.DeleteDataLakeObject(ctx, name)
}

func (s *IntegrationSuite) TestCreateEngagementDLO() {
	ctx := context.Background()
	name := "sftest_events__dll"

	_ = s.Client.DeleteDataLakeObject(ctx, name)

	req := &types.DataLakeObjectRequest{
		Name:     name,
		Label:    "sftest Events DLO",
		Category: types.CategoryEngagement,
		FieldInputRepresentations: []types.DataLakeField{
			{Name: "event_id__c", Label: "Event ID", DataType: "Text", IsPrimaryKey: true},
			{Name: "customer_id__c", Label: "Customer ID", DataType: "Text", IsPrimaryKey: false},
			{Name: "event_type__c", Label: "Event Type", DataType: "Text", IsPrimaryKey: false},
			{Name: "event_ts__c", Label: "Event Timestamp", DataType: "DateTime", IsPrimaryKey: false},
		},
		EventDateTimeFieldName: "event_ts__c",
	}

	dlo, err := s.Client.CreateDataLakeObject(ctx, req)
	if err != nil {
		sfErr, ok := err.(*SalesforceError)
		if ok && sfErr.Code == "INVALID_INPUT" {
			s.T().Skipf("DLO still exists from previous run: %v", err)
		}
		s.Require().NoError(err)
	}
	s.T().Logf("Created Engagement DLO: %s (id=%s)", dlo.Name, dlo.ID)

	_ = s.Client.DeleteDataLakeObject(ctx, name)
}

// --- Data Transform ---

func (s *IntegrationSuite) TestValidateTransformWithSchemaInference() {
	ctx := context.Background()

	// Get a source entity for the query
	meta, err := s.Client.GetMetadata(ctx, &types.MetadataRequest{})
	s.Require().NoError(err)
	s.Require().NotEmpty(meta.Metadata)
	sourceEntity := meta.Metadata[0].Name

	transformName := "sftest_infer_schema"
	targetName := "sftest_infer_output__dll"

	req := &types.DataTransformRequest{
		Name:  transformName,
		Label: "sftest Infer Schema",
		Type:  types.DataTransformTypeBatch,
		Definition: types.DataTransformDefinition{
			Type:    types.DataTransformDefinitionTypeDCSQL,
			Version: "1.0",
			Manifest: types.DataTransformManifest{
				Nodes: types.DataTransformNodes{
					types.DataTransformNodeID(targetName): {
						Name:         targetName,
						RelationName: targetName,
						Config: types.DataTransformNodeConfig{
							Materialized: types.MaterializationTable,
						},
						CompiledCode: "SELECT * FROM \"" + sourceEntity + "\" LIMIT 1",
					},
				},
			},
		},
	}

	result, err := s.Client.ValidateDataTransform(ctx, req)
	s.Require().NoError(err)

	s.T().Logf("Validation: %d issues", len(result.Issues))
	for _, issue := range result.Issues {
		s.T().Logf("  [%s] %s: %s", issue.Severity, issue.Code, issue.Message)
	}

	// Validate should return inferred output data objects
	odos, ok := result.OutputDataObjects[transformName]
	s.True(ok, "expected output data objects keyed by transform name")
	s.NotEmpty(odos, "expected at least one output data object")

	for _, odo := range odos {
		s.T().Logf("  OutputDataObject: %s (type=%s)", odo.Name, odo.Type)
	}
}

// --- Runner ---

func TestIntegrationSuite(t *testing.T) {
	suite.Run(t, new(IntegrationSuite))
}
