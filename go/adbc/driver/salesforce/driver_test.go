package salesforce

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	sfapi "github.com/apache/arrow-adbc/go/adbc/driver/salesforce/api"
	sftypes "github.com/apache/arrow-adbc/go/adbc/driver/salesforce/api/types"
	"github.com/apache/arrow-adbc/go/adbc/validation"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/suite"
)

type SalesforceQuirks struct {
	mem *memory.CheckedAllocator

	// TODO: we should create a separate gosalesforce.Client in SetupDriver (similar to the Snowflake validation tests)
	// This will allow us to do the workaround work necessary for Create/Drop table
	config *sftypes.AuthConfig
	client *sfapi.Client
}

func (s *SalesforceQuirks) Alloc() memory.Allocator                     { return s.mem }
func (s *SalesforceQuirks) Catalog() string                             { return "" } // TODO: we'll need to figure these out eventually
func (s *SalesforceQuirks) DBSchema() string                            { return "" } // TODO: we'll need to figure these out eventually
func (s *SalesforceQuirks) BindParameter(_ int) string                  { return "?" }
func (s *SalesforceQuirks) SupportsBulkIngest(_ string) bool            { return false }
func (s *SalesforceQuirks) SupportsConcurrentStatements() bool          { return true }
func (s *SalesforceQuirks) SupportsCurrentCatalogSchema() bool          { return false }
func (s *SalesforceQuirks) SupportsDynamicParameterBinding() bool       { return false }
func (s *SalesforceQuirks) SupportsErrorIngestIncompatibleSchema() bool { return false }
func (s *SalesforceQuirks) SupportsExecuteSchema() bool                 { return false }
func (s *SalesforceQuirks) SupportsGetParameterSchema() bool            { return false }
func (s *SalesforceQuirks) SupportsGetSetOptions() bool                 { return true }
func (s *SalesforceQuirks) SupportsPartitionedData() bool               { return false }
func (s *SalesforceQuirks) SupportsStatistics() bool                    { return false }
func (s *SalesforceQuirks) SupportsTransactions() bool                  { return false }

func (s *SalesforceQuirks) SetupDriver(t *testing.T) adbc.Driver {
	keyPEM, err := os.ReadFile(os.Getenv("SFDC_CLIENT_PRIVATE_KEY_PATH"))
	if err != nil {
		t.Fatalf("failed to read private key: %v", err)
	}

	cfg := &sftypes.AuthConfig{
		LoginURL:      os.Getenv("SFDC_LOGIN_URL"),
		ClientID:      os.Getenv("SFDC_CLIENT_ID"),
		Username:      os.Getenv("SFDC_USERNAME"),
		PrivateKeyPEM: string(keyPEM),
	}

	client, err := sfapi.NewClient(cfg)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	err = client.Authenticate(t.Context())
	if err != nil {
		t.Fatalf("Failed to authenticate client: %v", err)
	}

	s.config = cfg
	s.client = client
	s.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)

	return NewDriver(s.mem)
}

func (s *SalesforceQuirks) TearDownDriver(t *testing.T, drv adbc.Driver) {
	s.mem.AssertSize(t, 0)
}

func hasRealCredentials() bool {
	return os.Getenv("SFDC_LOGIN_URL") != "" &&
		os.Getenv("SFDC_CLIENT_ID") != "" &&
		os.Getenv("SFDC_USERNAME") != "" &&
		os.Getenv("SFDC_CLIENT_PRIVATE_KEY_PATH") != ""
}

func (s *SalesforceQuirks) DatabaseOptions() map[string]string {
	return map[string]string{
		OptionStringAuthType:      OptionValueAuthTypeJwtBearer,
		OptionStringLoginURL:      s.config.LoginURL,
		OptionStringClientID:      s.config.ClientID,
		OptionStringUsername:      s.config.Username,
		OptionStringJWTPrivateKey: s.config.PrivateKeyPEM,
	}
}

func (s *SalesforceQuirks) GetMetadata(code adbc.InfoCode) any {
	switch code {
	case adbc.InfoVendorName:
		return "Salesforce"
	case adbc.InfoDriverName:
		return "ADBC Salesforce Driver - Go"
	case adbc.InfoDriverADBCVersion:
		return adbc.AdbcVersion1_1_0
	case adbc.InfoDriverVersion:
		return "(unknown or development build)"
	case adbc.InfoDriverArrowVersion:
		return "(unknown or development build)"
	case adbc.InfoVendorVersion:
		return "(unknown or development build)"
	case adbc.InfoVendorArrowVersion:
		return "(unknown or development build)"
	}
	return nil
}

// ensureSeedDLO creates the seed_workaround__dll DLO if it does not already exist.
// This DLO acts as a dummy upstream source for DataTransforms.
func (s *SalesforceQuirks) ensureSeedDLO(ctx context.Context) error {
	const seedName = "seed_workaround__dll"
	_, err := s.client.GetDataLakeObject(ctx, seedName)
	if err == nil {
		return nil
	}
	// Any error (including 404) means we try to create it.
	_, createErr := s.client.CreateDataLakeObject(ctx, &sftypes.DataLakeObjectRequest{
		Name:     seedName,
		Label:    seedName,
		Category: sftypes.CategoryProfile,
		DataspaceInfo: []sftypes.DataspaceInfo{
			{Name: "default"},
		},
		FieldInputRepresentations: []sftypes.DataLakeField{
			{
				Name:         "Id",
				Label:        "Id",
				DataType:     "Text",
				IsPrimaryKey: true,
			},
		},
	})
	return createErr
}

// arrowTypeToSFDLOType maps an Arrow data type to a Salesforce DLO DataType string.
func arrowTypeToSFDLOType(dt arrow.DataType) sftypes.DataType {
	switch dt.ID() {
	case arrow.STRING, arrow.LARGE_STRING, arrow.BINARY, arrow.LARGE_BINARY:
		return sftypes.DataTypeText
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
		arrow.FLOAT16, arrow.FLOAT32, arrow.FLOAT64:
		return sftypes.DataTypeNumber
	case arrow.BOOL:
		return sftypes.DataTypeBoolean
	case arrow.DATE32, arrow.DATE64:
		return sftypes.DataTypeDate // or DateOnly
	case arrow.TIMESTAMP:
		return sftypes.DataTypeDateTime
	default:
		return sftypes.DataTypeUnsupported // TODO: what could happen here?
	}
}

// formatSQLLiteral renders the value at index i in arr as a SQL literal.
func formatSQLLiteral(arr arrow.Array, i int) string {
	if arr.IsNull(i) {
		return "NULL"
	}
	switch a := arr.(type) {
	case *array.String:
		escaped := strings.ReplaceAll(a.Value(i), "'", "''")
		return "'" + escaped + "'"
	case *array.LargeString:
		escaped := strings.ReplaceAll(a.Value(i), "'", "''")
		return "'" + escaped + "'"
	case *array.Binary:
		escaped := strings.ReplaceAll(string(a.Value(i)), "'", "''")
		return "'" + escaped + "'"
	case *array.LargeBinary:
		escaped := strings.ReplaceAll(string(a.Value(i)), "'", "''")
		return "'" + escaped + "'"
	case *array.Boolean:
		if a.Value(i) {
			return "TRUE"
		}
		return "FALSE"
	case *array.Int8:
		return fmt.Sprintf("%d", a.Value(i))
	case *array.Int16:
		return fmt.Sprintf("%d", a.Value(i))
	case *array.Int32:
		return fmt.Sprintf("%d", a.Value(i))
	case *array.Int64:
		return fmt.Sprintf("%d", a.Value(i))
	case *array.Uint8:
		return fmt.Sprintf("%d", a.Value(i))
	case *array.Uint16:
		return fmt.Sprintf("%d", a.Value(i))
	case *array.Uint32:
		return fmt.Sprintf("%d", a.Value(i))
	case *array.Uint64:
		return fmt.Sprintf("%d", a.Value(i))
	case *array.Float32:
		return fmt.Sprintf("%g", a.Value(i))
	case *array.Float64:
		return fmt.Sprintf("%g", a.Value(i))
	case *array.Date32:
		// Days since Unix epoch
		t := a.Value(i).ToTime()
		return fmt.Sprintf("DATE '%s'", t.Format("2006-01-02"))
	case *array.Date64:
		t := a.Value(i).ToTime()
		return fmt.Sprintf("DATE '%s'", t.Format("2006-01-02"))
	case *array.Timestamp:
		tsType := a.DataType().(*arrow.TimestampType)
		t := a.Value(i).ToTime(tsType.Unit)
		return fmt.Sprintf("TIMESTAMP '%s'", t.UTC().Format("2006-01-02 15:04:05.000"))
	default:
		return "NULL"
	}
}

// recordBatchToValuesSQL builds SQL that produces the batch rows as output.
//
// Uses a dual-CTE structure required by Salesforce Data Cloud:
//   - dummy: anchors the transform to the seed DLO (API requirement)
//   - workaround: UNION ALL of SELECT literals, one per row
//
// Column names use the __c suffix that the DLO API adds to field names.
func recordBatchToValuesSQL(batch arrow.RecordBatch) string {
	schema := batch.Schema()
	numCols := schema.NumFields()
	numRows := int(batch.NumRows())

	colNames := make([]string, numCols)
	for i := 0; i < numCols; i++ {
		// DLO fields are stored with __c suffix by the Salesforce API.
		colNames[i] = schema.Field(i).Name + "__c"
	}

	var sb strings.Builder
	sb.WriteString("WITH\ndummy AS (SELECT NULL FROM seed_workaround__dll LIMIT 0),\nworkaround AS (\n")

	for r := 0; r < numRows; r++ {
		if r > 0 {
			sb.WriteString("  UNION ALL ")
		} else {
			sb.WriteString("  ")
		}
		sb.WriteString("SELECT ")
		for c := 0; c < numCols; c++ {
			if c > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(formatSQLLiteral(batch.Column(c), r))
			if r == 0 {
				// Alias columns only in the first SELECT.
				sb.WriteString(" AS ")
				sb.WriteString(colNames[c])
			}
		}
		sb.WriteString("\n")
	}

	sb.WriteString(")\nSELECT * FROM workaround")
	return sb.String()
}

// sanitizeName converts a name to one containing only alphanumerics and underscores.
var nonAlphanumUnderscore = regexp.MustCompile(`[^a-zA-Z0-9_]`)

func sanitizeName(name string) string {
	return nonAlphanumUnderscore.ReplaceAllString(name, "_")
}

// waitForDLOActive polls GetDataLakeObject until Status becomes Active.
func (s *SalesforceQuirks) waitForDLOActive(ctx context.Context, name string) error {
	for {
		dlo, err := s.client.GetDataLakeObject(ctx, name)
		if err != nil {
			return fmt.Errorf("waitForDLOActive GetDataLakeObject(%s): %w", name, err)
		}
		if dlo.IsActive() {
			return nil
		}
		if dlo.IsError() {
			return fmt.Errorf("DLO %s entered error status", name)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(3 * time.Second):
		}
	}
}

// waitForTransformActive polls GetDataTransform until Status becomes Active.
func (s *SalesforceQuirks) waitForTransformActive(ctx context.Context, name string) error {
	for {
		dt, err := s.client.GetDataTransform(ctx, name)
		if err != nil {
			return fmt.Errorf("waitForTransformActive GetDataTransform(%s): %w", name, err)
		}
		if dt.Status.IsActive() {
			return nil
		}
		if dt.Status.IsError() {
			return fmt.Errorf("data transform %s entered error status", name)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(15 * time.Second):
		}
	}
}

// waitForTransform polls RefreshDataTransformStatus until the run reaches a terminal state.
func (s *SalesforceQuirks) waitForTransform(ctx context.Context, name string) error {
	for {
		dt, err := s.client.GetDataTransform(ctx, name)
		if err != nil {
			return fmt.Errorf("waitForTransform GetDataTransform(%s): %w", name, err)
		}
		status := dt.LastRunStatus
		if status.IsTerminal() {
			if status.IsSuccess() {
				return nil
			}
			fmt.Printf("[waitForTransform] FAILURE details: status=%s errorCode=%v date=%s id=%s\n", dt.LastRunStatus, dt.LastRunErrorCode, dt.LastRunDate, dt.ID)
			return fmt.Errorf("data transform %s ended with status %s (errorCode=%v, date=%s)", name, status, dt.LastRunErrorCode, dt.LastRunDate)
		}
		// Trigger a status refresh then wait briefly before polling again.
		_, _ = s.client.RefreshDataTransformStatus(ctx, name)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Minute):
		}
	}
}

// CreateSampleTable creates a DLO and populates it using a DataTransform with a VALUES CTE.
// The function is idempotent: it skips creation of any resource that already exists.
func (s *SalesforceQuirks) CreateSampleTable(name string, batch arrow.RecordBatch) error {
	ctx := context.Background()
	transformName := sanitizeName(name + "__seed")

	// Step 1: Create target DLO from batch schema (skip if it already exists).
	if _, existErr := s.client.GetDataLakeObject(ctx, name); existErr != nil {
		schema := batch.Schema()
		fields := make([]sftypes.DataLakeField, schema.NumFields())
		for i := 0; i < schema.NumFields(); i++ {
			f := schema.Field(i)
			fields[i] = sftypes.DataLakeField{
				Name:         f.Name,
				Label:        f.Name,
				DataType:     arrowTypeToSFDLOType(f.Type),
				IsPrimaryKey: i == 0,
			}
		}
		if _, err := s.client.CreateDataLakeObject(ctx, &sftypes.DataLakeObjectRequest{
			Name:     name,
			Label:    name,
			Category: sftypes.CategoryProfile,
			DataspaceInfo: []sftypes.DataspaceInfo{
				{Name: "default"},
			},
			FieldInputRepresentations: fields,
		}); err != nil {
			return fmt.Errorf("CreateSampleTable: create target DLO %s: %w", name, err)
		}
		if err := s.waitForDLOActive(ctx, name); err != nil {
			return fmt.Errorf("CreateSampleTable: waitForDLOActive %s: %w", name, err)
		}
	}

	// Step 2: Ensure seed DLO exists (required as a dummy upstream for the transform).
	if err := s.ensureSeedDLO(ctx); err != nil {
		return fmt.Errorf("CreateSampleTable: ensure seed DLO: %w", err)
	}

	// Step 3: Build VALUES CTE SQL — a single node with two CTEs:
	//   dummy: anchors the transform to the seed DLO (required by the API)
	//   workaround: the actual data rows
	valuesSQL := recordBatchToValuesSQL(batch)

	// Step 4: Create the DataTransform (skip if it already exists).
	if _, existErr := s.client.GetDataTransform(ctx, transformName); existErr != nil {
		req := &sftypes.DataTransformRequest{
			Name:          transformName,
			Label:         transformName,
			Type:          sftypes.DataTransformTypeBatch,
			DataSpaceName: "default",
			Definition: sftypes.DataTransformDefinition{
				Type:    sftypes.DataTransformDefinitionTypeDCSQL,
				Version: "1.0",
				Manifest: sftypes.DataTransformManifest{
					Nodes: sftypes.DataTransformNodes{
						sftypes.DataTransformNodeID(name): sftypes.DataTransformNode{
							Name:         name,
							RelationName: name + "__dll",
							CompiledCode: valuesSQL,
							Config: sftypes.DataTransformNodeConfig{
								Materialized: sftypes.MaterializationTable,
							},
						},
					},
				},
			},
		}
		if _, err := s.client.CreateDataTransform(ctx, req); err != nil {
			return fmt.Errorf("CreateSampleTable: create DataTransform %s: %w", transformName, err)
		}
	}

	// Step 5: Wait for the transform to become Active before running.
	if err := s.waitForTransformActive(ctx, transformName); err != nil {
		return fmt.Errorf("CreateSampleTable: waitForTransformActive %s: %w", transformName, err)
	}

	// Step 6: Run the DataTransform and wait for completion.
	if _, err := s.client.RunDataTransform(ctx, transformName); err != nil {
		return fmt.Errorf("CreateSampleTable: run DataTransform %s: %w", transformName, err)
	}
	if err := s.waitForTransform(ctx, transformName); err != nil {
		return fmt.Errorf("CreateSampleTable: waitForTransform %s: %w", transformName, err)
	}
	return nil
}

func isNotFound(err error) bool {
	var sfErr *sfapi.SalesforceError
	return errors.As(err, &sfErr) && sfErr.IsNotFound()
}

func (s *SalesforceQuirks) DropTable(_ adbc.Connection, name string) error {
	ctx := context.Background()
	transformName := sanitizeName(name + "__seed")

	if err := s.client.DeleteDataTransform(ctx, transformName); err != nil && !isNotFound(err) {
		return fmt.Errorf("DropTable: delete DataTransform %s: %w", transformName, err)
	}
	if err := s.client.DeleteDataLakeObject(ctx, name); err != nil && !isNotFound(err) {
		return fmt.Errorf("DropTable: delete DLO %s: %w", name, err)
	}
	return nil
}

func (s *SalesforceQuirks) SampleTableSchemaMetadata(_ string, _ arrow.DataType) arrow.Metadata {
	return arrow.Metadata{}
}

var _ validation.DriverQuirks = (*SalesforceQuirks)(nil)

func TestValidation(t *testing.T) {
	t.Skip("We need to get other things working before we make this work")
	if !hasRealCredentials() {
		t.Skip("skipping ConnectionTests: SFDC_ credentials not set")
	}

	q := &SalesforceQuirks{}

	suite.Run(t, &validation.DatabaseTests{Quirks: q})
	suite.Run(t, &validation.ConnectionTests{Quirks: q})
	// suite.Run(t, &validation.StatementTests{Quirks: q})
}
