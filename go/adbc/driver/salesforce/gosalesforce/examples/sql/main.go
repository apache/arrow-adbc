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

	query := "SELECT * FROM \"CurrencyType_Home__dll\" LIMIT 10"
	fmt.Printf("Executing query: %s\n", query)

	// Using the Data Cloud Connector API (supposed to be more advanced than the Query API V2)
	fmt.Println("\n=== SQL Query Example (Data Cloud Connector API) ===")
	demonstrateSqlQuery(client, query)

	// Using the Query API V2
	demonstrateQueryV2(client, query)
}

func demonstrateSqlQuery(client *api.Client, query string) {
	fmt.Println("Executing query via the Data Cloud Connector API...")
	queryRequest := &api.SqlQueryRequest{
		SQL:      query,
		RowLimit: 5,
		// Dataspace:    "default",        // Not supported by original API
		// WorkloadName: "demonstrateSqlQuery", // Not supported by original API
	}

	response, err := client.ExecuteSqlQuery(context.Background(), queryRequest)
	if err != nil {
		fmt.Printf("ERROR: Query execution failed: %v\n", err)
		return
	}

	shared.PrettyPrintJSON(response)
	fmt.Println("\n✅ Query executed successfully (via the Data Cloud Connector API)!")
}

func demonstrateQueryV2(client *api.Client, query string) {
	fmt.Println("Executing query via the Query API V2...")
	response, err := api.ExecuteSqlQueryLegacy(context.Background(), client, query, false)
	if err != nil {
		fmt.Printf("ERROR: Query V2 execution failed: %v\n", err)
		return
	}

	shared.PrettyPrintJSON(response)
	fmt.Println("\n✅ Query V2 executed successfully (via the Query API V2)!")
}
