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
	MAX_ELAPSED_TIME = 5 * time.Minute
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

	operation := func() (interface{}, error) {
		_, err := client.GetDataLakeObjectByName(ctx, name)
		if err != nil {
			return nil, nil
		}

		// DLO still exists
		if !deletionTriggered {
			deleteErr := client.DeleteDataLakeObjectByName(ctx, name)
			if deleteErr != nil {
				return nil, backoff.Permanent(fmt.Errorf("failed to delete existing DLO: %w", deleteErr))
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
		return fmt.Errorf("timeout waiting for DLO deletion: %w", err)
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

	operation := func() (interface{}, error) {
		_, err := client.GetDataTransform(ctx, name)
		if err != nil {
			// Data Transform doesn't exist, deletion complete or not needed
			return nil, nil
		}

		// Data Transform still exists
		if !deletionTriggered {
			deleteErr := client.DeleteDataTransform(ctx, name)
			if deleteErr != nil {
				return nil, backoff.Permanent(fmt.Errorf("failed to delete existing Data Transform: %w", deleteErr))
			}
			deletionTriggered = true
		}

		// Return retriable error to continue polling
		return nil, fmt.Errorf("data Transform %s still exists, waiting for deletion to complete", name)
	}

	// Retry with exponential backoff
	_, err := backoff.Retry(ctx, operation,
		backoff.WithBackOff(exponentialBackOff),
		backoff.WithMaxElapsedTime(MAX_ELAPSED_TIME),
		backoff.WithNotify(func(err error, duration time.Duration) {
			log.Printf("🕒 Data Transform deletion in progress, retrying in %v...\n", duration)
		}))

	if err != nil {
		return fmt.Errorf("timeout waiting for Data Transform deletion: %w", err)
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
	return dataLakeObject, nil
}

// If recreateIfExists is true, delete the existing data transform and create a new one before running it.
// Otherwise, run the existing data transform.
func (client *Client) TriggerDbtBatchDataTransform(ctx context.Context, targetDlo *DataLakeObject, sql string, recreateIfExists bool) (*DataTransform, error) {
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
			fmt.Sprintf("Create the target DLO %s", targetDlo.Name),
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
	waitForActiveOp := func() (interface{}, error) {
		// Eagerly refreshes status, otherwise `client.GetDataTransform` may respond with a stale status
		refreshStatusResponse, err := client.RefreshDataTransformStatus(ctx, dataTransform.Name)
		if err != nil {
			return nil, err
		}
		if !refreshStatusResponse.Success {
			return nil, fmt.Errorf("DCSQL transform status refresh failed: %v", refreshStatusResponse.Errors)
		}

		dataTransform, err := client.GetDataTransform(ctx, dataTransform.Name)
		if err != nil {
			return nil, err
		}
		if dataTransform.IsActive() {
			return nil, nil
		} else if dataTransform.IsError() {
			return nil, fmt.Errorf("DCSQL transform error: %v", dataTransform.Status)
		} else {
			return nil, fmt.Errorf("DCSQL transform is not active")
		}
	}

	exponentialBackOff.InitialInterval = INITIAL_INTERVAL
	exponentialBackOff.MaxInterval = MAX_INTERVAL
	_, err = backoff.Retry(ctx, waitForActiveOp,
		backoff.WithBackOff(exponentialBackOff),
		backoff.WithMaxElapsedTime(MAX_ELAPSED_TIME),
		backoff.WithNotify(func(err error, duration time.Duration) {
			log.Printf("🕒 DCSQL transform action in progress, retrying in %v...\n", duration)
		}))
	if err != nil {
		return nil, fmt.Errorf("timeout waiting for DCSQL transform action: %w", err)
	}

	// Runs the data transform
	_, err = client.RunDataTransform(ctx, dataTransform.Name)
	if err != nil {
		return nil, err
	}

	// Waits for the data transform run to be success
	waitForRunOp := func() (interface{}, error) {
		refreshStatusResponse, err := client.RefreshDataTransformStatus(ctx, dataTransform.Name)
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
			return nil, fmt.Errorf("DCSQL transform last run did not complete successfully: %v", refreshStatusResponse.Errors)
		}
		if !refreshStatusResponse.Success {
			return nil, fmt.Errorf("DCSQL transform status refresh failed: %v", refreshStatusResponse.Errors)
		}
		return nil, fmt.Errorf("DCSQL transform is not running")
	}

	exponentialBackOff.MaxInterval = 5 * time.Second // data transform run takes longer to complete
	_, err = backoff.Retry(ctx, waitForRunOp,
		backoff.WithBackOff(exponentialBackOff),
		backoff.WithMaxElapsedTime(MAX_ELAPSED_TIME),
		backoff.WithNotify(func(err error, duration time.Duration) {
			log.Printf("🕒 DCSQL transform run in progress, retrying in %v...\n", duration)
		}))
	if err != nil {
		return nil, fmt.Errorf("timeout waiting for DCSQL transform run: %w", err)
	}

	return dataTransform, nil
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
