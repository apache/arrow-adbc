package main

import (
	"context"
	"fmt"

	api "github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce/api"
	shared "github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce/shared"
)

func main() {
	fmt.Println("Salesforce Data Cloud SDK Examples")
	fmt.Println("==========================================")

	client, err := api.NewClientWithJWT()
	if err != nil {
		fmt.Printf("JWT Auth failed: %v\n", err)
		return
	}

	demonstrateIngestion(client)
}

// demonstrateIngestion shows how to use the Data Cloud ingestion APIs
func demonstrateIngestion(client *api.Client) {
	fmt.Println("Demonstrating Data Cloud ingestion workflow...")

	ctx := context.Background()

	// Step 1: Create a job
	fmt.Println("\n--- Step 1: Creating ingestion job ---")
	jobRequest := &api.CreateJobRequest{
		Object:     "Order",
		SourceName: "test_upload",
		Operation:  "upsert",
	}

	jobResp, err := api.CreateJob(ctx, client, jobRequest)
	if err != nil {
		fmt.Printf("ERROR: Job creation failed with CDP token: %v\n", err)
		return
	}

	fmt.Printf("Job created successfully!\n")
	shared.PrettyPrintJSON(jobResp)

	// Step 2: Prepare sample CSV data (API only supports CSV format)
	fmt.Println("\n--- Step 2: Preparing CSV data ---")
	csvData := `id,contact_name,created_date,total,shipAddress,taxExempt,tax_rate,my_email,my_phone,my_url,my_percent,is_new,modifie_date
ORD-001,John Doe,2025-01-15T10:30:00Z,150.75,123 Main St New York NY 10001,false,0.08,john.doe@example.com,555-0101,https://johndoe.com,10%,true,2025-01-15T10:30:00Z
ORD-002,Jane Smith,2025-01-16T14:20:00Z,299.99,456 Oak Ave Los Angeles CA 90210,true,0.00,jane.smith@example.com,555-0102,https://janesmith.co,15%,false,2025-01-16T14:20:00Z
ORD-003,Bob Johnson,2025-01-17T09:15:00Z,75.50,789 Pine St Chicago IL 60601,false,0.09,bob.johnson@example.com,555-0103,https://bobjohnson.net,5%,true,2025-01-17T09:15:00Z
ORD-004,Alice Williams,2025-01-18T16:45:00Z,425.25,321 Elm Dr Miami FL 33101,false,0.07,alice.williams@example.com,555-0104,https://alicew.org,20%,false,2025-01-18T16:45:00Z
ORD-005,Charlie Brown,2025-01-19T11:10:00Z,89.99,654 Maple Ln Seattle WA 98101,true,0.00,charlie.brown@example.com,555-0105,https://charlieb.io,12%,true,2025-01-19T11:10:00Z`

	// Step 3: Upload CSV data to the job
	fmt.Println("\n--- Step 3: Uploading CSV data ---")
	err = api.UploadJobData(ctx, client, jobResp.ID, []byte(csvData), "text/csv")
	if err != nil {
		fmt.Printf("ERROR: Data upload failed: %v\n", err)
		return
	} else {
		fmt.Println("Data uploaded successfully!")
	}

	// Step 4: Close the job to queue it for processing
	fmt.Println("\n--- Step 4: Closing job for processing ---")
	closeResp, err := api.CloseJob(ctx, client, jobResp.ID, "UploadComplete")
	if err != nil {
		fmt.Printf("ERROR: Job close failed: %v\n", err)
		return
	}

	fmt.Printf("Job closed successfully!\n")
	shared.PrettyPrintJSON(closeResp)

	// Example: Demonstrate abort functionality (may conflict with existing jobs)
	fmt.Println("\n--- Example: Demonstrating abort functionality ---")
	abortJobRequest := &api.CreateJobRequest{
		Object:     "OrderItem",
		SourceName: "test_abort", // Different source name to avoid conflicts
		Operation:  "upsert",
	}

	createJobResp, err := api.CreateJob(ctx, client, abortJobRequest)
	if err != nil {
		fmt.Printf("NOTE: Cannot create second job (expected in some orgs): %v\n", err)
	} else {
		fmt.Printf("Job created for abort example:\n")

		// Abort the job (simulating a cancellation scenario)
		closeResp, err := api.CloseJob(ctx, client, createJobResp.ID, "Aborted")
		shared.PrettyPrintJSON(closeResp)
		if err != nil {
			fmt.Printf("ERROR: Job abort failed: %v\n", err)
		} else {
			fmt.Printf("Job aborted successfully!\n")
		}
	}

	fmt.Println("\n✅ Complete ingestion workflow demonstration finished!")
}
