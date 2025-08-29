package shared

import (
	"context"
	"fmt"
	"log"
	"os"

	api "github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce/pkg"
)

func GetEnvOrPanic(key string) string {
	value := os.Getenv(key)
	if value == "" {
		log.Fatalf("Environment variable %s is required. Please set it before running this example.", key)
	}
	return value
}

func DemonstrateJWTAuth() (*api.Client, error) {
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

	config, err := api.NewJWTConfig(
		"https://login.salesforce.com",
		GetEnvOrPanic("SALESFORCE_CLIENT_ID"),
		"storm.050b6314da1346@salesforce.com",
		string(privateKey),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create JWT config: %w", err)
	}

	// Create client
	client := api.NewClient(config, "v64.0")

	// Authenticate
	fmt.Println("Connecting to Salesforce CDP with JWT...")
	err = client.Authenticate(context.Background())
	if err != nil {
		return nil, fmt.Errorf("authentication failed: %w", err)
	}

	fmt.Println("Connection successful!")
	printTokenInfo(client.GetToken())

	fmt.Println("\nTesting CDP token exchange...")
	err = client.ExchangeAndSetDataCloudToken(context.Background())
	if err != nil {
		fmt.Printf("WARNING: CDP token exchange failed: %v\n", err)
		fmt.Println("   This might be expected if CDP is not enabled for your org")
		return client, nil
	} else {
		fmt.Println("CDP token exchange successful!")
		fmt.Printf("CDP Instance URL: %s\n", client.GetDataCloudToken().InstanceURL)
		return client, nil
	}
}

func printTokenInfo(token *api.Token) {
	fmt.Printf("Token Information:\n")
	fmt.Printf("   Instance URL: %s\n", token.InstanceURL)
	fmt.Printf("   Token Type: %s\n", token.TokenType)
	fmt.Printf("   Access Token: %s\n", token.AccessToken)
	fmt.Printf("   Expires At: %s\n", token.ExpiresAt)
	fmt.Printf("   Salesforce Instance: %s\n", api.GetInstanceFromToken(token))
	fmt.Printf("   Has Refresh Token: %t\n", token.RefreshToken != "")
}
