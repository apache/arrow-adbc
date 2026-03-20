package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc/driver/salesforce/api/types"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gopkg.in/dnaeon/go-vcr.v4/pkg/cassette"
	"gopkg.in/dnaeon/go-vcr.v4/pkg/recorder"
	"resty.dev/v3"
)

// NOTE: hasRealCredentials() and realAuthConfig() are defined in testing_helpers_test.go

type FlowsSuite struct {
	suite.Suite
	Client   *Client
	recorder *recorder.Recorder
}

func TestFlows(t *testing.T) {
	if !hasRealCredentials() {
		t.Skip("skipping flow tests: no Salesforce credentials")
	}
	suite.Run(t, new(FlowsSuite))
}

func parseURL(raw string) *url.URL {
	u, _ := url.Parse(raw)
	return u
}

var (
	hookStripAuthHeader = recorder.WithHook(
		func(i *cassette.Interaction) error {
			delete(i.Request.Headers, "Authorization")
			return nil
		},
		recorder.BeforeSaveHook,
	)

	hookPrettyJsonBody = recorder.WithHook(
		func(i *cassette.Interaction) error {
			var (
				t   string
				err error
				buf bytes.Buffer
			)

			t, _, err = mime.ParseMediaType(i.Request.Headers.Get("Content-Type"))
			if err == nil && t == "application/json" {
				buf.Reset()
				err = json.Indent(&buf, []byte(i.Request.Body), "", "  ")
				if err == nil {
					i.Request.Body = buf.String()
				}
			}

			t, _, err = mime.ParseMediaType(i.Response.Headers.Get("Content-Type"))
			if err == nil && t == "application/json" {
				buf.Reset()
				err = json.Indent(&buf, []byte(i.Response.Body), "", "  ")
				if err == nil {
					i.Response.Body = buf.String()
				}
			}

			return nil
		},
		recorder.BeforeSaveHook,
	)
)

func (s *FlowsSuite) SetupTest() {
	t := s.T()
	logger := slog.New(slog.NewTextHandler(t.Output(), &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	cassetteName := filepath.Join("testdata", strings.ReplaceAll(t.Name(), "/", "_"))

	mode := recorder.ModeRecordOnly

	r, err := recorder.New(cassetteName,
		recorder.WithMode(mode),
		recorder.WithSkipRequestLatency(true),
		hookStripAuthHeader,
		hookPrettyJsonBody,
		recorder.WithMatcher(func(r *http.Request, i cassette.Request) bool {
			if r.Method != i.Method {
				return false
			}
			cassetteURL := parseURL(i.URL)
			return r.URL.Path == cassetteURL.Path && r.URL.RawQuery == cassetteURL.RawQuery
		}),
	)
	require.NoError(t, err)
	s.recorder = r

	withVCR := WithModifyClient(func(c *resty.Client) {
		c.SetTransport(r)
		c.SetHeader("Accept-Encoding", "identity")
	})

	if hasRealCredentials() {
		cfg := realAuthConfig(t)
		client, err := NewClient(cfg, withVCR, WithLogger(logger))
		require.NoError(t, err)

		err = client.Authenticate(context.Background())
		require.NoError(t, err)

		s.Client = client
	} else {
		client, err := NewClient(
			&types.AuthConfig{
				LoginURL:   "https://test.salesforce.com",
				ClientID:   "test-client-id",
				Username:   "test@example.com",
				APIVersion: "v64.0",
			},
			withVCR,
			WithLogger(logger),
		)
		require.NoError(t, err)

		client.SetBaseURL("https://test.salesforce.com")
		client.SetAuthToken("test-token")

		s.Client = client
	}
}

func (s *FlowsSuite) TearDownTest() {
	if s.recorder != nil {
		err := s.recorder.Stop()
		s.Require().NoError(err)
	}
	if s.Client != nil {
		s.Client.Close()
	}
}

func buildTransformRequest(transformName, outputDLOName, sqlQuery string) *types.DataTransformRequest {
	return &types.DataTransformRequest{
		Name:          transformName,
		Label:         transformName,
		Type:          types.DataTransformTypeBatch,
		DataSpaceName: "default",
		Definition: types.DataTransformDefinition{
			Type:    types.DataTransformDefinitionTypeDCSQL,
			Version: "1.0",
			Manifest: types.DataTransformManifest{
				Nodes: types.DataTransformNodes{
					types.DataTransformNodeID("node_" + outputDLOName): {
						Name:         outputDLOName,
						RelationName: outputDLOName,
						Config: types.DataTransformNodeConfig{
							Materialized: types.MaterializationTable,
							WriteMode:    types.WriteModeOverwrite,
						},
						CompiledCode: sqlQuery,
					},
				},
			},
		},
	}
}

// cleanupResources deletes a transform and DLO, waiting for both to be fully gone.
// Used at the start of each test to remove leftovers from prior failed runs.
func (s *FlowsSuite) cleanupResources(ctx context.Context, transformName, dloName string) {
	t := s.T()
	if err := s.Client.DeleteTransformAndWait(ctx, transformName, nil); err != nil {
		t.Logf("cleanup: failed to delete transform %q: %v", transformName, err)
	}
	if err := s.Client.DeleteDLOAndWait(ctx, dloName, nil); err != nil {
		t.Logf("cleanup: failed to delete DLO %q: %v", dloName, err)
	}
}

func (s *FlowsSuite) TestCreateRunDeleteTransform() {
	ctx := context.Background()
	transformName := "flow_test_create_run_transform"
	outputDLO := "flow_test_create_run__dll"
	sql := "SELECT primary_key__c, fieldtype_Text__c FROM test_all_field_types__dll"

	s.cleanupResources(ctx, transformName, outputDLO)

	req := buildTransformRequest(transformName, outputDLO, sql)

	// Create via validate + create
	dt, _, err := s.Client.ValidateAndCreateTransform(ctx, req, "primary_key__c")
	s.Require().NoError(err)
	s.Require().NotNil(dt)

	// Wait for Active
	dt, err = s.Client.WaitForTransformStatus(ctx, transformName, nil, types.StatusActive, types.StatusError)
	s.Require().NoError(err)
	s.Require().True(dt.Status.IsActive())

	// Run and wait
	dt, err = s.Client.RunAndWaitForTransform(ctx, transformName, nil)
	s.Require().NoError(err)
	s.Require().True(dt.LastRunStatus.IsSuccess())

	// Cleanup
	err = s.Client.DeleteTransformAndWait(ctx, transformName, nil)
	s.Require().NoError(err)

	err = s.Client.DeleteDLOAndWait(ctx, outputDLO, nil)
	s.Require().NoError(err)
}

func (s *FlowsSuite) TestValidateAndCreateWithAutoCreateDLO() {
	ctx := context.Background()
	transformName := "flow_test_validate_create_transform"
	outputDLO := "flow_test_validate_create__dll"
	sql := "SELECT primary_key__c, fieldtype_Email__c FROM test_all_field_types__dll"

	s.cleanupResources(ctx, transformName, outputDLO)

	req := buildTransformRequest(transformName, outputDLO, sql)

	// Validate and create
	dt, _, err := s.Client.ValidateAndCreateTransform(ctx, req, "primary_key__c")
	s.Require().NoError(err)
	s.Require().NotNil(dt)

	// Wait for Active
	dt, err = s.Client.WaitForTransformStatus(ctx, transformName, nil, types.StatusActive, types.StatusError)
	s.Require().NoError(err)
	s.Require().True(dt.Status.IsActive())

	// Run and wait
	dt, err = s.Client.RunAndWaitForTransform(ctx, transformName, nil)
	s.Require().NoError(err)
	s.Require().True(dt.LastRunStatus.IsSuccess())

	// Cleanup
	err = s.Client.DeleteTransformAndWait(ctx, transformName, nil)
	s.Require().NoError(err)

	err = s.Client.DeleteDLOAndWait(ctx, outputDLO, nil)
	s.Require().NoError(err)
}

func (s *FlowsSuite) TestDeleteTransformAndWait() {
	ctx := context.Background()
	transformName := "flow_test_delete_transform"
	outputDLO := "flow_test_delete__dll"
	sql := "SELECT primary_key__c FROM test_all_field_types__dll"

	s.cleanupResources(ctx, transformName, outputDLO)

	req := buildTransformRequest(transformName, outputDLO, sql)

	// Create transform
	dt, _, err := s.Client.ValidateAndCreateTransform(ctx, req, "primary_key__c")
	s.Require().NoError(err)
	s.Require().NotNil(dt)

	// Wait for Active
	_, err = s.Client.WaitForTransformStatus(ctx, transformName, nil, types.StatusActive, types.StatusError)
	s.Require().NoError(err)

	// Delete and wait
	err = s.Client.DeleteTransformAndWait(ctx, transformName, nil)
	s.Require().NoError(err)

	// Verify 404
	_, err = s.Client.GetDataTransform(ctx, transformName)
	s.Require().Error(err)
	var sfErr *SalesforceError
	s.Require().True(errors.As(err, &sfErr))
	s.Require().True(sfErr.IsNotFound())

	// Cleanup output DLO
	err = s.Client.DeleteDLOAndWait(ctx, outputDLO, nil)
	s.Require().NoError(err)
}
