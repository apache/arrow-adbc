package main

import (
	"context"
	"fmt"

	api "github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce/api"
)

func main() {
	fmt.Println("Salesforce Data Cloud SDK Examples")
	fmt.Println("==========================================")

	client, err := api.NewClientWithJWT()
	if err != nil {
		fmt.Printf("JWT Auth failed: %v\n", err)
		return
	}

	demonstrateMetadata(client)
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
