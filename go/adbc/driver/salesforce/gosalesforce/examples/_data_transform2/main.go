package main

import (
	"context"
	"fmt"
	"log"
	"slices"
	"time"

	api "github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce/api"
	shared "github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce/shared"
)

func main() {
	fmt.Println("Salesforce Data Cloud - Data Transform Examples")
	fmt.Println("===============================================")

	fmt.Println("\n=== JWT Authentication ===")
	client, err := api.NewClientWithJWT()
	if err != nil {
		log.Fatalf("JWT Auth failed: %v", err)
		return
	}
	fmt.Println("\n=== Data Transform API Example ===")
	demonstrateDCSQLDataTransform(client)
}

func demonstrateDCSQLDataTransform(client *api.Client) error {
	ctx := context.Background()

	// client.HttpOptions(ctx, "/services/data/v65.0/")

	// // Deletes if it exists
	// if _, err := client.GetDataTransform(ctx, name); err == nil {
	// 	fmt.Printf("Data transform %s already exists, deleting...\n", name)
	// 	client.DeleteDataTransform(ctx, name)
	// }

	name := "demonstrateDCSQLDataTransformAuto"
	targetName := "customers_stg_auto__dll"

	// err := client.DeleteDataTransformIfExists(ctx, name)
	// if err != nil {
	// 	fmt.Printf("ERROR: Failed to delete Data Transform: %v\n", err)
	// 	return err
	// }

	request := api.NewBatchDataTransformRequest(
		name,
		"Example Demo DCSQL Data Transform Auto ",
		map[string]api.DbtDataTransformNode{
			"node": api.NewSimpleDbtDataTransformNode(
				"node",
				targetName,
				"SELECT \"CustomerId__c\", \"CustomerId__c\" AS another_customer_id FROM \"customers_raw__dll\"",
			),
		},
	)

	validation, err := client.ValidateDataTransform(ctx, request)
	if err != nil {
		fmt.Printf("ERROR: DCSQL transform validation failed: %v\n", err)
		return err
	}

	odos, ok := validation.OutputDataObjects[name]
	if !ok {
		return fmt.Errorf("ERROR: Something went wrong: missing name key")
	}

	odoIdx := slices.IndexFunc(odos, func(odo api.OutputDataObject) bool {
		return odo.Name == targetName
	})

	if odoIdx < 0 {
		return fmt.Errorf("ERROR: Something went wrong: missing matching output data object ")
	}

	odo := &odos[odoIdx]
	if odo.Category == "" {
		odo.Category = api.CategoryProfile
	}
	if odo.Label == "" {
		odo.Label = odo.Name
	}
	for i := range odo.Fields {
		r := &odo.Fields[i]

		if i == 0 {
			r.IsPrimaryKey = true
		}

		if r.Label == "" {
			r.Label = r.Name
		}
	}

	shared.PrettyPrintJSON(odo)

	request.Definition.OutputDataObjects = odos

	// return nil

	// Creates a data transform
	dataTransform, err := client.CreateDataTransform(ctx, request)
	if err != nil {
		fmt.Printf("ERROR: DCSQL transform creation failed: %v\n", err)
		return err
	}
	fmt.Printf("✅ Advanced DCSQL transform created successfully!\n")
	shared.PrettyPrintJSON(dataTransform)

	return nil
	// Waits for the data transform to be active
	for {
		dataTransform, err := client.GetDataTransform(ctx, dataTransform.Name)
		if err != nil {
			fmt.Printf("ERROR: DCSQL transform get failed: %v\n", err)
			return err
		}
		if dataTransform.IsActive() {
			fmt.Printf("✅ DCSQL transform is active\n")
			break
		} else if dataTransform.IsError() {
			fmt.Printf("ERROR: DCSQL transform error: %v\n", dataTransform.Status)
			return err
		}

		fmt.Println("🕒 Waiting 1 seconds for status to update...")
		time.Sleep(1 * time.Second)

		// Eagerly refreshes status, otherwise `client.GetDataTransform` may respond with a stale status
		refreshStatusResponse, err := client.RefreshDataTransformStatus(ctx, dataTransform.Name)
		if err != nil {
			fmt.Printf("ERROR: DCSQL transform status refresh failed: %v\n", err)
			return err
		}
		if !refreshStatusResponse.Success {
			// ignores the non-success response
			// noticed that RefreshDataTransformStatus always returns a non-success response
			// when invoked immediately after the data transform is created
			fmt.Printf("WARNING: DCSQL transform status refresh failed: %v\n", refreshStatusResponse.Errors)
		}
	}

	// Runs the data transform
	runResponse, err := client.RunDataTransform(ctx, dataTransform.Name)
	if err != nil {
		fmt.Printf("ERROR: DCSQL transform run failed: %v\n", err)
		return err
	}
	if !runResponse.Success {
		fmt.Printf("ERROR: DCSQL transform run failed: %v\n", runResponse.Errors)
		return err
	}
	fmt.Printf("✅ DCSQL transform run is triggered successfully!\n")

	// Waits for the data transform to be active
	for {
		dataTransform, err := client.GetDataTransform(ctx, dataTransform.Name)
		if err != nil {
			fmt.Printf("ERROR: DCSQL transform get failed: %v\n", err)
			return err
		}
		if dataTransform.IsLastRunSuccess() {
			fmt.Printf("✅ DCSQL transform last run successfully!\n")
			break
		} else if dataTransform.IsLastRunFailure() || dataTransform.IsLastRunCanceled() {
			fmt.Printf("ERROR: DCSQL transform last run did not complete successfully: %v\n", dataTransform.LastRunStatus)
			return err
		}

		fmt.Println("🕒 Waiting 5 seconds for status to update...")
		time.Sleep(5 * time.Second)

		refreshStatusResponse, err := client.RefreshDataTransformStatus(ctx, dataTransform.Name)
		if err != nil {
			fmt.Printf("ERROR: DCSQL transform status refresh failed: %v\n", err)
			return err
		}
		if !refreshStatusResponse.Success {
			fmt.Printf("WARNING: DCSQL transform status refresh failed: %v\n", refreshStatusResponse.Errors)
		}
	}

	return nil
}
