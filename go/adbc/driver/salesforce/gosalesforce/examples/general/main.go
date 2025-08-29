/*
Example usage of the Salesforce Data Cloud SDK.

This example demonstrates:
- JWT authentication
- SQL query execution
- Query V2 API
- Metadata API

Required environment variables:
- SALESFORCE_CLIENT_ID: Your Salesforce Connected App Consumer Key

Example usage:

	export SALESFORCE_CLIENT_ID="your_consumer_key_here"
	go run examples/main.go
*/
package main

import (
	"context"
	"fmt"
	"log"

	shared "github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce/examples/shared"
	api "github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce/pkg"
)

func main() {
	fmt.Println("Salesforce Data Cloud SDK Examples")
	fmt.Println("==========================================")

	// Example 1: JWT Authentication (matches Python implementation)
	fmt.Println("\n=== JWT Authentication ===")
	client, err := shared.DemonstrateJWTAuth()
	if err != nil {
		log.Fatalf("JWT Auth failed: %v", err)
		return
	}

	// SQL Query Execution
	// Example 2: Data Cloud Connector API
	fmt.Println("\n=== SQL Query Example (Data Cloud Connector API) ===")
	demonstrateSqlQuery(client)

	// Example 3: Query API V2
	// fmt.Println("\n=== SQL Query Example (Query API V2) ===")
	demonstrateQueryV2(client)

	// Example 4: Metadata API
	// fmt.Println("\n=== Metadata API Example ===")
	demonstrateMetadata(client)

	// Example 5: Data Ingestion API
	// fmt.Println("\n=== Data Ingestion API Example ===")
	demonstrateIngestion(client)

	// Example 6: Data Transform API
	fmt.Println("\n=== Data Transform API Example ===")
	demonstrateDataTransform(client)
}

// Helper function to display row data consistently
func displayRowData(data [][]interface{}, description string) {
	if len(data) > 0 {
		fmt.Printf("\n%s (%d rows):\n", description, len(data))
		for i, row := range data {
			fmt.Printf("   Row %d: ", i+1)
			for j, value := range row {
				if j > 0 {
					fmt.Print(", ")
				}
				if value == nil {
					fmt.Print("NULL")
				} else {
					fmt.Printf("%v", value)
				}
			}
			fmt.Println()
		}
	} else {
		fmt.Printf("\n%s: No data returned\n", description)
	}
}

func demonstrateSqlQuery(client *api.Client) {
	rowLimit := int64(5)
	queryRequest := &api.SqlQueryRequest{
		SQL:      "SELECT * FROM \"CurrencyType_Home__dll\" LIMIT 10",
		RowLimit: &rowLimit,
		// Dataspace:    "default",        // Not supported by original API
		// WorkloadName: "demonstrateSqlQuery", // Not supported by original API
	}

	response, err := client.ExecuteSqlQuery(context.Background(), queryRequest)
	if err != nil {
		fmt.Printf("ERROR: Query execution failed: %v\n", err)
		return
	}

	fmt.Println("\nQuery executed successfully!")
	fmt.Printf("Query Results:\n")
	fmt.Printf("   Returned rows: %d\n", response.ReturnedRows)
	fmt.Printf("   Total rows: %d\n", response.Status.RowCount)
	fmt.Printf("   Query ID: %s\n", response.Status.QueryId)
	fmt.Printf("   Completion status: %s\n", response.Status.CompletionStatus)
	fmt.Printf("   Progress: %.1f%%\n", response.Status.Progress*100)

	// Display column metadata
	fmt.Printf("\nColumn Metadata (%d columns):\n", len(response.Metadata))
	for i, col := range response.Metadata {
		nullable := "NOT NULL"
		if col.Nullable {
			nullable = "NULLABLE"
		}
		if col.Precision != nil && col.Scale != nil {
			fmt.Printf("   %d. %s (%s, precision=%d, scale=%d) %s\n",
				i+1, col.Name, col.Type, *col.Precision, *col.Scale, nullable)
		} else {
			fmt.Printf("   %d. %s (%s) %s\n", i+1, col.Name, col.Type, nullable)
		}
	}

	// Display sample data
	displayRowData(response.Data, "Sample Data")
}

func demonstrateQueryV2(client *api.Client) {
	fmt.Println("SQL Query execution example (V2 API)")
	fmt.Println("Executing a real SQL query against Data Cloud using v2 API")

	query := "SELECT 1"
	fmt.Printf("Executing query: %s\n", query)

	// Execute the query using v2 API
	response, err := api.ExecuteSqlQueryLegacy(context.Background(), client, query, false)
	if err != nil {
		fmt.Printf("ERROR: Query V2 execution failed: %v\n", err)
		return
	}

	fmt.Println("\nQuery V2 executed successfully!")
	displayQueryV2Results(response)
}

func displayQueryV2Results(response *api.QueryV2Response) {
	fmt.Printf("Query V2 Results:\n")
	if response.RowCount > 0 {
		fmt.Printf("   Row count: %d\n", response.RowCount)
	}
	if response.QueryId != "" {
		fmt.Printf("   Query ID: %s\n", response.QueryId)
	}
	fmt.Printf("   Done: %t\n", response.Done)
	if response.NextBatchId != nil && *response.NextBatchId != "" {
		fmt.Printf("   Next batch ID: %s\n", *response.NextBatchId)
	}
	if response.StartTime != "" {
		fmt.Printf("   Start time: %s\n", response.StartTime)
	}
	if response.EndTime != "" {
		fmt.Printf("   End time: %s\n", response.EndTime)
	}

	// Display column metadata
	if len(response.Metadata) > 0 {
		fmt.Printf("\nColumn Metadata (%d columns):\n", len(response.Metadata))
		for columnName, metadata := range response.Metadata {
			fmt.Printf("   %s: %s (type code: %d, order: %d)\n",
				columnName, metadata.Type, metadata.TypeCode, metadata.PlaceInOrder)
		}
	}

	// Display sample data
	displayRowData(response.Data, "Data")
}

// demonstrateMetadata shows how to retrieve Data Cloud metadata
func demonstrateMetadata(client *api.Client) {
	fmt.Println("Retrieving Data Cloud metadata...")

	ctx := context.Background()

	// Get all metadata first
	fmt.Println("Fetching all metadata...")
	metadataResp, err := api.GetMetadata(ctx, client, "", "", "", "")
	if err != nil {
		fmt.Printf("ERROR: Metadata retrieval failed: %v\n", err)
		return
	}

	fmt.Printf("Metadata retrieved successfully!\n")
	fmt.Printf("Total entities: %d\n", len(metadataResp.Metadata))

	// Display a summary of entities
	if len(metadataResp.Metadata) > 0 {
		fmt.Printf("\nFirst 10 entities:\n")
		maxDisplay := len(metadataResp.Metadata)
		if maxDisplay > 10 {
			maxDisplay = 10
		}

		for i := 0; i < maxDisplay; i++ {
			entity := metadataResp.Metadata[i]
			entityType := "Unknown"
			if len(entity.Fields) > 0 {
				entityType = "DataLakeObject/DataModelObject"
			} else if len(entity.Dimensions) > 0 || len(entity.Measures) > 0 {
				entityType = "CalculatedInsight"
			}

			fmt.Printf("   %d. %s (%s) - %s\n", i+1, entity.Name, entity.DisplayName, entityType)
			if entity.Category != "" {
				fmt.Printf("      Category: %s\n", entity.Category)
			}
			if len(entity.Fields) > 0 {
				fmt.Printf("      Fields: %d\n", len(entity.Fields))
			}
			if len(entity.Dimensions) > 0 {
				fmt.Printf("      Dimensions: %d\n", len(entity.Dimensions))
			}
			if len(entity.Measures) > 0 {
				fmt.Printf("      Measures: %d\n", len(entity.Measures))
			}
			if len(entity.Relationships) > 0 {
				fmt.Printf("      Relationships: %d\n", len(entity.Relationships))
			}
		}

		if len(metadataResp.Metadata) > 10 {
			fmt.Printf("   ... and %d more entities\n", len(metadataResp.Metadata)-10)
		}
	}

	// Try to get specific entity metadata if we found any
	if len(metadataResp.Metadata) > 0 {
		firstEntity := metadataResp.Metadata[0]
		fmt.Printf("\n--- Detailed view of first entity: %s ---\n", firstEntity.Name)

		if len(firstEntity.Fields) > 0 {
			fmt.Printf("\nFields (%d):\n", len(firstEntity.Fields))
			maxFields := len(firstEntity.Fields)
			if maxFields > 5 {
				maxFields = 5
			}
			for i := 0; i < maxFields; i++ {
				field := firstEntity.Fields[i]
				nullable := "NOT NULL"
				if field.Nullable {
					nullable = "NULLABLE"
				}
				fmt.Printf("   %s (%s) %s", field.Name, field.Type, nullable)
				if field.DisplayName != "" && field.DisplayName != field.Name {
					fmt.Printf(" - %s", field.DisplayName)
				}
				fmt.Println()
			}
			if len(firstEntity.Fields) > 5 {
				fmt.Printf("   ... and %d more fields\n", len(firstEntity.Fields)-5)
			}
		}

		if len(firstEntity.Relationships) > 0 {
			fmt.Printf("\nRelationships (%d):\n", len(firstEntity.Relationships))
			maxRels := len(firstEntity.Relationships)
			if maxRels > 3 {
				maxRels = 3
			}
			for i := 0; i < maxRels; i++ {
				rel := firstEntity.Relationships[i]
				fmt.Printf("   %s.%s -> %s.%s (%s)\n",
					rel.FromEntity, rel.FromEntityAttribute,
					rel.ToEntity, rel.ToEntityAttribute,
					rel.Cardinality)
			}
			if len(firstEntity.Relationships) > 3 {
				fmt.Printf("   ... and %d more relationships\n", len(firstEntity.Relationships)-3)
			}
		}
	}

	// Example with entity type filter
	fmt.Println("\n--- Fetching only Profile entities ---")
	profileResp, err := api.GetMetadata(ctx, client, "", "Profile", "", "")
	if err != nil {
		fmt.Printf("ERROR: Profile metadata retrieval failed with original token: %v\n", err)

		// Try with CDP token
		profileResp, err = api.GetMetadata(ctx, client, "", "Profile", "", "")
		if err != nil {
			fmt.Printf("ERROR: Profile metadata retrieval also failed with CDP token: %v\n", err)
		}
	}

	if err == nil {
		fmt.Printf("Profile entities found: %d\n", len(profileResp.Metadata))
		if len(profileResp.Metadata) > 0 {
			for i, entity := range profileResp.Metadata {
				if i >= 3 {
					fmt.Printf("   ... and %d more profile entities\n", len(profileResp.Metadata)-3)
					break
				}
				fmt.Printf("   %s (%s)\n", entity.Name, entity.DisplayName)
			}
		}
	}
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
	fmt.Printf("   Job ID: %s\n", jobResp.ID)
	fmt.Printf("   State: %s\n", jobResp.State)
	fmt.Printf("   Object: %s\n", jobResp.Object)
	fmt.Printf("   Operation: %s\n", jobResp.Operation)
	fmt.Printf("   Source Name: %s\n", jobResp.SourceName)
	fmt.Printf("   Content Type: %s\n", jobResp.ContentType)
	fmt.Printf("   Content URL: %s\n", jobResp.ContentURL)

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
	fmt.Printf("   Job ID: %s\n", closeResp.ID)
	fmt.Printf("   State: %s\n", closeResp.State)
	fmt.Printf("   Created Date: %s\n", closeResp.CreatedDate)
	fmt.Printf("   System Modstamp: %s\n", closeResp.SystemModstamp)
	fmt.Printf("   Data is now queued for processing in Data Cloud\n")

	// Example: Demonstrate abort functionality (may conflict with existing jobs)
	fmt.Println("\n--- Example: Demonstrating abort functionality ---")
	abortJobRequest := &api.CreateJobRequest{
		Object:     "OrderItem",
		SourceName: "test_abort", // Different source name to avoid conflicts
		Operation:  "upsert",
	}

	abortJobResp, err := api.CreateJob(ctx, client, abortJobRequest)
	if err != nil {
		fmt.Printf("NOTE: Cannot create second job (expected in some orgs): %v\n", err)
		fmt.Printf("This is normal - many orgs limit concurrent jobs or duplicate configurations.\n")
		fmt.Printf("In production, you would typically create, upload, and close jobs sequentially.\n")
	} else {
		fmt.Printf("Job created for abort example:\n")
		fmt.Printf("   Job ID: %s\n", abortJobResp.ID)
		fmt.Printf("   State: %s\n", abortJobResp.State)

		// Abort the job (simulating a cancellation scenario)
		abortResp, err := api.CloseJob(ctx, client, abortJobResp.ID, "Aborted")
		if err != nil {
			fmt.Printf("ERROR: Job abort failed: %v\n", err)
		} else {
			fmt.Printf("Job aborted successfully!\n")
			fmt.Printf("   Job ID: %s\n", abortResp.ID)
			fmt.Printf("   State: %s\n", abortResp.State)
			fmt.Printf("   Any uploaded data has been deleted\n")
		}
	}

	fmt.Println("\n✅ Complete ingestion workflow demonstration finished!")
	fmt.Println("📊 Summary:")
	fmt.Println("   - Created job and uploaded 5 Order records")
	fmt.Println("   - Closed job for processing (data queued)")
	fmt.Println("   - Demonstrated job abort functionality")
	fmt.Println("   - Jobs can be monitored via Data Cloud UI or additional API calls")
}

// demonstrateDataTransform shows how to create data transforms in Data Cloud
func demonstrateDataTransform(client *api.Client) {
	fmt.Println("Demonstrating Data Cloud transform creation...")

	ctx := context.Background()

	// Verify tokens are available
	accessToken := client.GetToken()

	if accessToken == nil || accessToken.AccessToken == "" {
		fmt.Println("WARNING: No access token available")
		return
	}

	fmt.Printf("Using Salesforce instance: %s\n", accessToken.InstanceURL)

	// Example 1: Create a Batch Data Transform
	fmt.Println("\n--- Creating Batch Data Transform (STL) ---")
	err := demonstrateBatchTransform(ctx, client)
	if err != nil {
		fmt.Printf("ERROR: Batch transform creation failed: %v\n", err)
	} else {
		fmt.Println("✅ Batch transform creation successful!")
	}

	// Example 2: Create a Streaming Data Transform
	fmt.Println("\n--- Creating Streaming Data Transform (SQL) ---")
	err = demonstrateStreamingTransform(ctx, client)
	if err != nil {
		fmt.Printf("ERROR: Streaming transform creation failed: %v\n", err)
	} else {
		fmt.Println("✅ Streaming transform creation successful!")
	}

	fmt.Println("\n✅ Data Transform demonstration completed!")
	fmt.Println("📊 Summary:")
	fmt.Println("   - Demonstrated batch data transform creation (STL)")
	fmt.Println("   - Demonstrated streaming data transform creation (SQL)")
	fmt.Println("   - Transforms can be monitored and managed via Data Cloud UI")
}

func demonstrateBatchTransform(ctx context.Context, client *api.Client) error {
	// Create nodes for a batch transform that loads data and outputs it
	// Using actual data objects that exist in the org
	nodes := map[string]api.DataTransformNode{
		"LOAD_DATASET0": api.NewLoadDatasetNode(
			"CurrencyType_Home__dll",
			"dataLakeObject",
			[]string{"Id__c", "IsoCode__c"},
		),
		"OUTPUT0": api.NewOutputNode(
			"Currency_Transform_Output__dll",
			"dataLakeObject",
			[]api.DataTransformFieldMapping{
				api.NewFieldMapping("Id__c", "Id__c"),
				api.NewFieldMapping("IsoCode__c", "IsoCode__c"),
			},
			[]string{"LOAD_DATASET0"},
		),
	}

	// Create the batch transform request
	request := api.NewBatchDataTransformRequest(
		"BatchCurrencyCleaningExample",
		"Batch Currency Cleaning Example",
		nodes,
	)

	// Add optional fields
	request.Description = "Example batch data transform that cleans currency data"
	request.CreationType = api.DataTransformCreationTypeCustom

	fmt.Printf("Creating batch transform: %s\n", request.Name)
	fmt.Printf("   Label: %s\n", request.Label)
	fmt.Printf("   Type: %s\n", request.Type)
	fmt.Printf("   Definition Type: %s\n", request.Definition.Type)
	fmt.Printf("   Nodes: %d\n", len(request.Definition.Nodes))

	// Execute the request
	response, err := api.CreateDataTransform(ctx, client, request)
	if err != nil {
		return err
	}

	// Display results
	fmt.Printf("✅ Batch transform created successfully!\n")
	fmt.Printf("   Transform ID: %s\n", response.ID)
	fmt.Printf("   Name: %s\n", response.Name)
	fmt.Printf("   Label: %s\n", response.Label)
	fmt.Printf("   Status: %s\n", response.Status)
	fmt.Printf("   Type: %s\n", response.Type)
	fmt.Printf("   Created Date: %s\n", response.CreatedDate)
	fmt.Printf("   Created By: %s\n", response.CreatedBy.Name)
	fmt.Printf("   Last Run Status: %s\n", response.LastRunStatus)
	fmt.Printf("   URL: %s\n", response.URL)

	// Display available actions
	if response.ActionUrls.RunAction != "" {
		fmt.Printf("   Available Actions:\n")
		if response.ActionUrls.RunAction != "" {
			fmt.Printf("     - Run: %s\n", response.ActionUrls.RunAction)
		}
		if response.ActionUrls.CancelAction != "" {
			fmt.Printf("     - Cancel: %s\n", response.ActionUrls.CancelAction)
		}
		if response.ActionUrls.RetryAction != "" {
			fmt.Printf("     - Retry: %s\n", response.ActionUrls.RetryAction)
		}
		if response.ActionUrls.RefreshStatusAction != "" {
			fmt.Printf("     - Refresh Status: %s\n", response.ActionUrls.RefreshStatusAction)
		}
	}

	// Display output data objects if present
	if len(response.Definition.OutputDataObjects) > 0 {
		fmt.Printf("   Output Data Objects: %d\n", len(response.Definition.OutputDataObjects))
		for i, obj := range response.Definition.OutputDataObjects {
			fmt.Printf("     %d. %s (%s)\n", i+1, obj.Name, obj.Label)
			if obj.Status != "" {
				fmt.Printf("        Status: %s\n", obj.Status)
			}
			if len(obj.Fields) > 0 {
				fmt.Printf("        Fields: %d\n", len(obj.Fields))
			}
		}
	}

	return nil
}

func demonstrateStreamingTransform(ctx context.Context, client *api.Client) error {
	// Create the streaming transform request using existing data objects
	request := api.NewStreamingDataTransformRequest(
		"StreamingCurrencyTransformExample",
		"Streaming Currency Transform Example",
		"SELECT Id__c, IsoCode__c FROM CurrencyType_Home__dll",
		"Currency_Stream_Output__dll",
	)

	// Add optional fields
	request.Description = "Example streaming data transform that processes currency data in real-time"
	request.CreationType = api.DataTransformCreationTypeCustom

	fmt.Printf("Creating streaming transform: %s\n", request.Name)
	fmt.Printf("   Label: %s\n", request.Label)
	fmt.Printf("   Type: %s\n", request.Type)
	fmt.Printf("   Definition Type: %s\n", request.Definition.Type)
	fmt.Printf("   SQL Expression: %s\n", request.Definition.Expression)
	fmt.Printf("   Target DLO: %s\n", request.Definition.TargetDlo)

	// Execute the request
	response, err := api.CreateDataTransform(ctx, client, request)
	if err != nil {
		return err
	}

	// Display results
	fmt.Printf("✅ Streaming transform created successfully!\n")
	fmt.Printf("   Transform ID: %s\n", response.ID)
	fmt.Printf("   Name: %s\n", response.Name)
	fmt.Printf("   Label: %s\n", response.Label)
	fmt.Printf("   Status: %s\n", response.Status)
	fmt.Printf("   Type: %s\n", response.Type)
	fmt.Printf("   Created Date: %s\n", response.CreatedDate)
	fmt.Printf("   Created By: %s\n", response.CreatedBy.Name)
	fmt.Printf("   Last Run Status: %s\n", response.LastRunStatus)
	fmt.Printf("   URL: %s\n", response.URL)

	// Display the SQL expression from the definition
	if response.Definition.Expression != "" {
		fmt.Printf("   SQL Expression: %s\n", response.Definition.Expression)
	}
	if response.Definition.TargetDlo != "" {
		fmt.Printf("   Target DLO: %s\n", response.Definition.TargetDlo)
	}

	return nil
}
