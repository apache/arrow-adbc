package api

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce/shared"
	"github.com/cenkalti/backoff/v5"
)

// Constants for exponential backoff
const (
	MAX_ELAPSED_TIME = 3 * time.Minute
	INITIAL_INTERVAL = 100 * time.Millisecond
	MAX_INTERVAL     = 1 * time.Second
)

// DeleteIfDloExists deletes a DLO if it exists
func (client *Client) DeleteIfDloExists(ctx context.Context, name string) error {
	// Delete all data transforms that are targeting the DLO (see TODO on the client.GetDataTransformByDLO method)
	dataTransforms, err := client.GetDataTransformByDLO(ctx, name)
	if err != nil {
		return fmt.Errorf("failed to get data transform by DLO: %w", err)
	}

	for _, dataTransform := range dataTransforms {
		err = client.DeleteDataTransformIfExists(ctx, dataTransform.Name)
		if err != nil {
			return fmt.Errorf("failed to delete data transform: %w", err)
		}
	}

	// Delete the DLO using exponential backoff
	deletionTriggered := false

	// Configure exponential backoff
	exponentialBackOff := backoff.NewExponentialBackOff()
	exponentialBackOff.InitialInterval = INITIAL_INTERVAL
	exponentialBackOff.MaxInterval = MAX_INTERVAL

	operation := func() (any, error) {
		_, err := client.GetDataLakeObjectByName(ctx, name)
		if err != nil {
			return nil, nil
		}

		// DLO still exists
		if !deletionTriggered {
			deleteErr := client.DeleteDataLakeObjectByName(ctx, name)
			if deleteErr != nil {
				return nil, backoff.Permanent(fmt.Errorf("failed to delete the existing DLO %s: %w", name, deleteErr))
			}
			deletionTriggered = true
		}

		// Return retriable error to continue polling
		return nil, fmt.Errorf("DLO %s still exists, waiting for deletion to complete", name)
	}

	// Retry with exponential backoff
	_, err = backoff.Retry(ctx, operation,
		backoff.WithBackOff(exponentialBackOff),
		backoff.WithMaxElapsedTime(MAX_ELAPSED_TIME),
		backoff.WithNotify(func(err error, duration time.Duration) {
			log.Printf("🕒 DLO deletion in progress, retrying in %v...\n", duration)
		}))

	if err != nil {
		return err
	}

	return nil
}

// DeleteDataTransformIfExists deletes a Data Transform if it exists
func (client *Client) DeleteDataTransformIfExists(ctx context.Context, name string) error {
	deletionTriggered := false

	// Configure exponential backoff
	exponentialBackOff := backoff.NewExponentialBackOff()
	exponentialBackOff.InitialInterval = INITIAL_INTERVAL
	exponentialBackOff.MaxInterval = MAX_INTERVAL

	operation := func() (any, error) {
		_, err := client.GetDataTransform(ctx, name)
		if err != nil {
			// Data Transform doesn't exist, deletion complete or not needed
			return nil, nil
		}

		// Data Transform still exists
		if !deletionTriggered {
			deleteErr := client.DeleteDataTransform(ctx, name)
			if deleteErr != nil {
				return nil, backoff.Permanent(fmt.Errorf("failed to delete the existing data transform: %w", deleteErr))
			}
			deletionTriggered = true
		}

		// Return retryable error to continue polling
		return nil, fmt.Errorf("data transform %s is not deleted yet", name)
	}

	// Retry with exponential backoff
	_, err := backoff.Retry(ctx, operation,
		backoff.WithBackOff(exponentialBackOff),
		backoff.WithMaxElapsedTime(MAX_ELAPSED_TIME),
		backoff.WithNotify(func(err error, duration time.Duration) {
			log.Printf("🕒 data transform deletion in progress, retrying in %v...\n", duration)
		}))

	if err != nil {
		return err
	}

	return nil
}

// CreateDataLakeObject creates a Data Lake Object using the inferred schema via the SQL execution
//
// sql must be a Query, DDL or DML is disallowed by the Salesforce Data Cloud API
func (client *Client) CreateDataLakeObjectWithInferredSchema(ctx context.Context, sql string, dataSpace string, targetDLOName string, primaryKeyFieldName string, category DataLakeObjectCategory) (*DataLakeObject, error) {
	// Infer the target DLO schema using the SQL Query API
	queryRequest := &SqlQueryRequest{
		SQL:      sql,
		RowLimit: 0,
		// Dataspace:    "default",        // Not supported by original API
		// WorkloadName: "demonstrateSqlQuery", // Not supported by original API
	}
	sqlResponse, err := client.ExecuteSqlQuery(ctx, queryRequest)
	if err != nil {
		return nil, err
	}

	// Infer DLO request from SQL response metadata
	request := NewDataLakeObjectFromSqlResponse(
		targetDLOName,
		targetDLOName,
		category,
		sqlResponse,
		primaryKeyFieldName,
	)

	// Set required fields to empty as they're optional
	request.OrgUnitIdentifierFieldName = ""
	request.RecordModifiedFieldName = ""

	// This assigned the DLO to a data space
	request.DataspaceInfo = []DataspaceInfo{
		{
			Name: dataSpace,
		},
	}

	// Create the DLO
	dataLakeObject, err := client.PostDataLakeObject(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("failed to create DLO: %w", err)
	}

	// Wait for the DLO to be Active
	exponentialBackOff := backoff.NewExponentialBackOff()
	exponentialBackOff.InitialInterval = INITIAL_INTERVAL
	exponentialBackOff.MaxInterval = MAX_INTERVAL

	waitForActiveOp := func() (any, error) {
		currentDLO, err := client.GetDataLakeObject(ctx, dataLakeObject.Name, nil, nil, "")
		if err != nil {
			return nil, fmt.Errorf("failed to get DLO status: %w", err)
		}

		if currentDLO.IsActive() {
			return currentDLO, nil
		}

		if currentDLO.IsError() {
			return nil, backoff.Permanent(fmt.Errorf("DLO creation failed with error status"))
		}

		return nil, fmt.Errorf("DLO %s still in status %s, waiting for Active status", dataLakeObject.ID, currentDLO.Status)
	}
	result, err := backoff.Retry(ctx, waitForActiveOp,
		backoff.WithBackOff(exponentialBackOff),
		backoff.WithMaxElapsedTime(MAX_ELAPSED_TIME),
		backoff.WithNotify(func(err error, duration time.Duration) {
			log.Printf("🕒 DLO creation in progress, retrying in %v...\n", duration)
		}))

	if err != nil {
		return nil, err
	}

	// Return the active DLO
	return result.(*DataLakeObject), nil
}

// If recreateIfExists is true, delete the existing data transform and create a new one before running it.
// Otherwise, run the existing data transform.
func (client *Client) TriggerDbtBatchDataTransform(ctx context.Context, targetDlo *DataLakeObject, sql string, recreateIfExists bool, runTimeout time.Duration) (*DataTransform, error) {
	var dataTransform *DataTransform
	var err error
	if recreateIfExists {
		err := client.DeleteDataTransformIfExists(ctx, targetDlo.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to delete Data Transform: %w", err)
		}

		// Creates a data transform
		request := NewBatchDataTransformRequest(
			targetDlo.Name,
			targetDlo.Name,
			map[string]DbtDataTransformNode{
				"node": NewSimpleDbtDataTransformNode(
					"node",
					targetDlo.Name,
					sql,
				),
			},
		)

		dataTransform, err = client.CreateDataTransform(ctx, request)
		if err != nil {
			return nil, err
		}
	} else {
		dataTransform, err = client.GetDataTransform(ctx, targetDlo.Name)
		if err != nil {
			return nil, err
		}
	}

	// Configure exponential backoff
	exponentialBackOff := backoff.NewExponentialBackOff()

	// Waits for the data transform to be active
	waitForActiveOp := func() (any, error) {
		// Eagerly refreshes status, otherwise `client.GetDataTransform` may respond with a stale status
		refreshStatusResponse, err := client.RefreshDataTransformStatus(ctx, dataTransform.Name)
		if err != nil {
			return nil, err
		}
		if !refreshStatusResponse.Success {
			return nil, fmt.Errorf("data transform status refresh failed with errors %v", refreshStatusResponse.Errors)
		}

		dataTransform, err := client.GetDataTransform(ctx, dataTransform.Name)
		if err != nil {
			return nil, err
		}

		if dataTransform.IsActive() {
			return nil, nil
		} else if dataTransform.IsError() {
			return nil, fmt.Errorf("failed to create the data transform, settled status [%v]", dataTransform.Status)
		} else {
			return nil, fmt.Errorf("waiting for data transform %s to be active times out, it is still in status [%v]", dataTransform.Name, dataTransform.Status)
		}
	}

	exponentialBackOff.InitialInterval = INITIAL_INTERVAL
	exponentialBackOff.MaxInterval = MAX_INTERVAL
	_, err = backoff.Retry(ctx, waitForActiveOp,
		backoff.WithBackOff(exponentialBackOff),
		backoff.WithMaxElapsedTime(MAX_ELAPSED_TIME),
		backoff.WithNotify(func(err error, duration time.Duration) {
			log.Printf("🕒 data transform run in progress, retrying in %v...\n", duration)
		}))
	if err != nil {
		return nil, err
	}

	// Runs the data transform
	_, err = client.RunDataTransform(ctx, dataTransform.Name)
	if err != nil {
		return nil, err
	}

	// Waits for the data transform run to be success
	waitForRunOp := func() (any, error) {
		refreshStatusResponse, err := client.RefreshDataTransformStatus(ctx, dataTransform.Name)
		if !refreshStatusResponse.Success {
			return nil, fmt.Errorf("failed to refresh the data transform status [%v]", refreshStatusResponse.Errors)
		}
		if err != nil {
			return nil, err
		}

		dataTransform, err := client.GetDataTransform(ctx, dataTransform.Name)
		if err != nil {
			return nil, err
		}

		if dataTransform.IsLastRunSuccess() {
			return nil, nil
		} else if dataTransform.IsLastRunFailure() || dataTransform.IsLastRunCanceled() {
			return nil, fmt.Errorf("failed to complete the data transform with errors %v", refreshStatusResponse.Errors)
		} else {
			return nil, fmt.Errorf("data transform run times out, it is still in status [%v]", dataTransform.LastRunStatus)
		}
	}

	exponentialBackOff.MaxInterval = 5 * time.Second // data transform run takes longer to complete
	_, err = backoff.Retry(ctx, waitForRunOp,
		backoff.WithBackOff(exponentialBackOff),
		backoff.WithMaxElapsedTime(runTimeout),
		backoff.WithNotify(func(err error, duration time.Duration) {
			log.Printf("🕒 data transform run in progress, retrying in %v...\n", duration)
		}))

	if err != nil {
		log.Printf("⚠️ data transform run failed, cancelling data transform %s\n", dataTransform.Name)
		// Use background context for cancellation to ensure it completes even if parent ctx is cancelled
		_, cancelErr := client.CancelDataTransform(context.Background(), dataTransform.Name)
		if cancelErr != nil {
			log.Printf("⚠️ data transform cancellation failed: %v", cancelErr)
		}
		return nil, err
	}

	return dataTransform, nil
}

// Polls the data transform status until it is no longer processing.
// Returns the latest data transform received from the server.
func (client *Client) WaitForDataTransform(ctx context.Context, dt *DataTransform) (*DataTransform, error) {
	client.logger.DebugContext(ctx, "WaitForDataTransform")

	op := func() (*DataTransform, error) {
		ndt, err := client.GetDataTransform(ctx, dt.Name)
		if err != nil {
			return nil, backoff.Permanent(err)
		}

		if ndt.Status.IsProcessing() {
			return ndt, fmt.Errorf("still processing")
		}

		return ndt, nil
	}

	policy := backoff.NewExponentialBackOff()
	policy.InitialInterval = 1 * time.Second
	policy.MaxInterval = 15 * time.Second

	return backoff.Retry(
		ctx,
		op,
		backoff.WithBackOff(policy),
		backoff.WithMaxElapsedTime(MAX_ELAPSED_TIME),
		backoff.WithNotify(func(err error, duration time.Duration) {
			client.logger.DebugContext(ctx, "waiting", "err", err, "duration", duration)
		}),
	)
}

// Polls the data transform last run status until it is no longer pending or in-progress.
// Returns the latest data transform received from the server.
func (client *Client) WaitForDataTransformRun(ctx context.Context, dt *DataTransform, runTimeout time.Duration) (*DataTransform, error) {
	client.logger.DebugContext(ctx, "WaitForDataTransformRun", "timeout", runTimeout)
	op := func() (*DataTransform, error) {
		err := client.MustRefreshDataTransformStatus(ctx, dt.Name)
		if err != nil {
			// return nil, backoff.Permanent(err)
			return nil, err
		}

		ndt, err := client.GetDataTransform(ctx, dt.Name)
		if err != nil {
			return nil, backoff.Permanent(err)
		}

		switch {
		case ndt.LastRunStatus.IsPending():
			return ndt, fmt.Errorf("run pending")
		case ndt.LastRunStatus.IsInProgress():
			return ndt, fmt.Errorf("run in progress")
		}

		return ndt, nil
	}

	policy := backoff.NewExponentialBackOff()
	policy.InitialInterval = 30 * time.Second
	policy.MaxInterval = 15 * time.Second

	return backoff.Retry(
		ctx,
		op,
		backoff.WithBackOff(policy),
		backoff.WithMaxElapsedTime(runTimeout),
	)
}

func (client *Client) CreateOrUpdateDataTransform(ctx context.Context, req *CreateDataTransformRequest) (*DataTransform, error) {
	l := client.logger.With("operation", "CreateOrUpdateDataTransform")

	l.DebugContext(ctx, "checking DT exists")

	// naively check if the data-transform already exists
	// TODO: There there are errors that can occur if the data transform exists (i.e. rate limiting, transient server issue, etc)
	if _, err := client.GetDataTransform(ctx, req.Name); err != nil {
		// TODO: What if existing.Status == DELETING | PROGRESSING
		l.DebugContext(ctx, "creating data transform", "name", req.Name)
		return client.CreateDataTransform(ctx, req)
	} else {
		l.DebugContext(ctx, "updating data transform", "name", req.Name)
		return client.UpdateDataTransform(ctx, req)
	}

}

type DataSpaceMember struct {
	Name   string        `json:"memberName"`
	Filter *FilterConfig `json:"filter,omitzero"`
}

func (client *Client) UpsertDataSpaceMembers(ctx context.Context, dataSpace string, members []DataSpaceMember) (*DataCloudActionResponse, error) {
	type upsertDataspaceMemberBody struct {
		Members struct {
			Members []DataSpaceMember `json:"members"`
		} `json:"members"`
	}

	type upsertDataspaceMemberResp struct {
		*DataCloudActionResponse
		Members struct {
			Members []DataSpaceMember `json:"members"`
		} `json:"dataSpaceMembers"`
	}

	var reqBody upsertDataspaceMemberBody
	reqBody.Members.Members = members

	path := fmt.Sprintf("data-spoaces/%s/members", dataSpace)
	resp, err := PutJSON[upsertDataspaceMemberBody, upsertDataspaceMemberResp](client, ctx, path, &reqBody)
	if err != nil {
		return nil, err
	}

	return resp.DataCloudActionResponse, nil
}

// NewClientWithJWT creates a new client using JWT authentication
// It expects the private key to be stored in the home directory at `~/.salesforce/JWT/server.key`
// It expects the login URL to be stored in the environment variable `SALESFORCE_LOGIN_URL`
// It expects the client ID to be stored in the environment variable `SALESFORCE_CLIENT_ID`
// It expects the username to be stored in the environment variable `SALESFORCE_USERNAME`
func NewClientWithJWT() (*Client, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("failed to get home directory: %w", err)
	}

	privateKeyPath := fmt.Sprintf("%s/salesforce/JWT/server.key", home)
	if _, err := os.Stat(privateKeyPath); os.IsNotExist(err) {
		fmt.Printf("WARNING: Private key file not found at: %s\n", privateKeyPath)
		fmt.Println("   Please ensure the private key file exists or update the path")
		return nil, nil
	}

	privateKey, err := os.ReadFile(privateKeyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read private key file: %w", err)
	}

	config, err := NewJWTConfig(
		shared.GetEnvOrPanic("SALESFORCE_LOGIN_URL"),
		shared.GetEnvOrPanic("SALESFORCE_CLIENT_ID"),
		shared.GetEnvOrPanic("SALESFORCE_USERNAME"),
		string(privateKey),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create JWT config: %w", err)
	}

	// Create client
	client := NewClient(config, "v64.0")

	// Authenticate
	err = client.Authenticate(context.Background())
	if err != nil {
		return nil, fmt.Errorf("authentication failed: %w", err)
	}

	err = client.ExchangeAndSetDataCloudToken(context.Background())
	if err != nil {
		return client, nil
	} else {
		return client, nil
	}
}
