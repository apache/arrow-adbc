package salesforce

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	sfapi "github.com/apache/arrow-adbc/go/adbc/driver/salesforce/api"
	sftypes "github.com/apache/arrow-adbc/go/adbc/driver/salesforce/api/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gopkg.in/dnaeon/go-vcr.v4/pkg/cassette"
	"gopkg.in/dnaeon/go-vcr.v4/pkg/recorder"
	"resty.dev/v3"
)

// DriverSuite is a VCR-backed integration suite for driver-level behaviour.
//
// When SFDC_ credentials are set it records to testdata/ (ModeRecordOnly).
// Without credentials it replays from existing cassettes (ModeReplayOnly).
type DriverSuite struct {
	suite.Suite
	client   *sfapi.Client
	recorder *recorder.Recorder
}

func (s *DriverSuite) SetupTest() {
	t := s.T()
	cassetteName := filepath.Join("testdata", strings.ReplaceAll(t.Name(), "/", "_"))

	r, err := recorder.New(cassetteName,
		recorder.WithMode(recorder.ModeRecordOnly),
		recorder.WithSkipRequestLatency(true),
		recorder.WithHook(func(i *cassette.Interaction) error {
			delete(i.Request.Headers, "Authorization")
			return nil
		}, recorder.BeforeSaveHook),
		recorder.WithMatcher(func(r *http.Request, i cassette.Request) bool {
			if r.Method != i.Method {
				return false
			}
			requestPath := r.URL.Path
			if r.URL.RawQuery != "" {
				requestPath += "?" + r.URL.RawQuery
			}
			cassetteURL := i.URL
			if idx := strings.Index(cassetteURL, "//"); idx != -1 {
				if pathIdx := strings.Index(cassetteURL[idx+2:], "/"); pathIdx != -1 {
					cassetteURL = cassetteURL[idx+2+pathIdx:]
				}
			}
			return requestPath == cassetteURL
		}),
	)
	require.NoError(t, err)
	s.recorder = r

	withVCR := sfapi.WithModifyClient(func(c *resty.Client) {
		// Use vcr transport
		c.SetTransport(r)
		// Disable gzip so cassettes store plain text (readable + replayable)
		c.SetHeader("Accept-Encoding", "identity")
	})

	cfg := driverSuiteAuthConfig(t)
	client, err := sfapi.NewClient(cfg, withVCR)
	require.NoError(t, err)
	require.NoError(t, client.Authenticate(context.Background()))
	s.client = client
}

func (s *DriverSuite) TearDownTest() {
	if s.recorder != nil {
		s.Require().NoError(s.recorder.Stop())
	}
	if s.client != nil {
		s.client.Close()
	}
}

func driverSuiteAuthConfig(t require.TestingT) *sftypes.AuthConfig {
	keyPEM, err := os.ReadFile(os.Getenv("SFDC_CLIENT_PRIVATE_KEY_PATH"))
	require.NoError(t, err)
	return &sftypes.AuthConfig{
		LoginURL:      os.Getenv("SFDC_LOGIN_URL"),
		ClientID:      os.Getenv("SFDC_CLIENT_ID"),
		Username:      os.Getenv("SFDC_USERNAME"),
		PrivateKeyPEM: string(keyPEM),
	}
}

// TestCreateDLOFromRecordBatch exercises the full CreateSampleTable / DropTable
// workaround end-to-end against a real (or recorded) Salesforce org.
func (s *DriverSuite) TestCreateDLOFromRecordBatch() {
	ctx := context.Background()
	const tableName = "driver_suite_sample"

	q := &SalesforceQuirks{client: s.client}

	// Build a simple two-column RecordBatch: ints (int32) + strings (utf8).
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(s.T(), 0)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "ints", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "strings", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	ib := array.NewInt32Builder(mem)
	defer ib.Release()
	ib.AppendValues([]int32{1, 2, 3}, []bool{true, true, true})
	ints := ib.NewArray()
	defer ints.Release()

	sb := array.NewStringBuilder(mem)
	defer sb.Release()
	sb.AppendValues([]string{"a", "b", "c"}, []bool{true, true, true})
	strs := sb.NewArray()
	defer strs.Release()

	batch := array.NewRecordBatch(schema, []arrow.Array{ints, strs}, 3)
	defer batch.Release()

	// Ensure no leftover from a previous run.
	_ = q.DropTable(nil, tableName)

	s.Require().NoError(q.CreateSampleTable(tableName, batch))

	// Verify the DLO exists.
	dlo, err := s.client.GetDataLakeObject(ctx, tableName)
	s.Require().NoError(err)
	s.Require().Equal("driver_suite_sample__dll", dlo.Name)

	// Clean up.
	s.Require().NoError(q.DropTable(nil, tableName))
}

func TestDriverSuite(t *testing.T) {
	t.Skip("We need to get other things working before we make this work")
	if !hasRealCredentials() {
		t.Skip("skipping DriverSuite: SFDC_ credentials not set and no cassettes recorded yet")
	}
	suite.Run(t, new(DriverSuite))
}
