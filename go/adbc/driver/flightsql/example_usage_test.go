// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// RECIPE STARTS HERE

// Tests that use the SQLite server example.

package flightsql_test

import (
	"context"
	"net/http"
	"os/exec"
	"time"

	// "database/sql"
	"fmt"
	"log"

	"github.com/apache/arrow-adbc/go/adbc"
	drv "github.com/apache/arrow-adbc/go/adbc/driver/flightsql"
	"github.com/apache/arrow-go/v18/arrow/array"
	"golang.org/x/oauth2"

	// "github.com/apache/arrow-go/v18/arrow/flight"
	// "github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	// sqlite "github.com/apache/arrow-go/v18/arrow/flight/flightsql/example"
	"github.com/apache/arrow-go/v18/arrow/memory"
	_ "modernc.org/sqlite"
)

var headers = map[string]string{"foo": "bar"}

func redirectHandler(w http.ResponseWriter, r *http.Request) {
	// Extract query parameters from the redirect URI
	code := r.URL.Query().Get("code")
	state := r.URL.Query().Get("state")

	// Print the received code and state
	fmt.Printf("Received code: %s\n", code)
	fmt.Printf("Received state: %s\n", state)

	// Respond to the client
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Authorization code received. You can close this window."))
	stopChan <- code
}

var stopChan = make(chan string)

func getToken(ctx context.Context) (string, error) {

	conf := &oauth2.Config{
		ClientID: "dremio-backend",
		// ClientSecret:
		RedirectURL: "http://localhost:8555/",
		// Scopes:
		Endpoint: oauth2.Endpoint{
			TokenURL:      "http://localhost:8080/realms/dremio-realm/protocol/openid-connect/token",
			AuthURL:       "http://localhost:8080/realms/dremio-realm/protocol/openid-connect/auth",
			DeviceAuthURL: "http://localhost:8080/realms/dremio-realm/protocol/openid-connect/auth/device",
			// AuthStyle:
		},
	}

	// use PKCE to protect against CSRF attacks
	// https://www.ietf.org/archive/id/draft-ietf-oauth-security-topics-22.html#name-countermeasures-6
	verifier := oauth2.GenerateVerifier()

	// Redirect user to consent page to ask for permission
	// for the scopes specified above.
	url := conf.AuthCodeURL("state", oauth2.AccessTypeOffline, oauth2.S256ChallengeOption(verifier))
	fmt.Printf("Visit the URL for the auth dialog: %v", url)

	// Open the browser with the authorization URL
	err := exec.Command("open", url).Start()
	if err != nil {
		fmt.Printf("Failed to open browser: %v\n", err)
	}

	http.HandleFunc("/", redirectHandler)

	// Start the HTTP server on port 8080
	srv := &http.Server{Addr: ":8555"}

	// Start the server in a goroutine
	go func() {
		fmt.Println("Starting server on port 8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("Error starting server: %v\n", err)
		}
	}()

	// Wait for the stop signal
	code := <-stopChan

	// Create a context with a timeout to allow the server to shut down gracefully
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Shutdown the server
	if err := srv.Shutdown(ctx); err != nil {
		fmt.Printf("Server forced to shutdown: %v\n", err)
	}

	fmt.Println("Server stopped")

	access_token, err := conf.Exchange(ctx, code, oauth2.VerifierOption(verifier))

	if err != nil {
		return "", err
	}

	return access_token.AccessToken, nil
}

func FlightSQLExample(uri string) error {
	ctx := context.Background()
	options := map[string]string{
		adbc.OptionKeyURI: uri,
	}

	for k, v := range headers {
		options[drv.OptionRPCCallHeaderPrefix+k] = v
	}
	// options[drv.OptionAuthorizationHeader] = "Basic ZHJlbWlvOmRyZW1pbzEyMw=="
	tok, err := getToken(ctx)
	if err != nil {
		return fmt.Errorf("failed to get token: %s", err.Error())
	}
	options["token"] = tok
	var alloc memory.Allocator
	drv := drv.NewDriver(alloc)
	db, err := drv.NewDatabase(options)
	if err != nil {
		return fmt.Errorf("failed to open database: %s\n", err.Error())
	}
	defer db.Close()

	cnxn, err := db.Open(ctx)
	if err != nil {
		return fmt.Errorf("failed to open connection: %s", err.Error())
	}
	defer cnxn.Close()

	rdr, err := cnxn.GetObjects(ctx, adbc.ObjectDepthAll, nil, nil, nil, nil, nil)

	if err != nil {
		return fmt.Errorf("failed to get objects: %s", err.Error())
	}
	defer rdr.Release()
	stmt, err := cnxn.NewStatement()
	if err != nil {
		return fmt.Errorf("failed to create statement: %s", err.Error())
	}
	defer stmt.Close()

	if err = stmt.SetSqlQuery("SELECT 1 AS theresult"); err != nil {
		return fmt.Errorf("failed to set query: %s", err.Error())
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return fmt.Errorf("failed to execute query: %s", err.Error())
	}
	defer reader.Release()

	for reader.Next() {
		arr, ok := reader.Record().Column(0).(*array.Int32)
		if !ok {
			return fmt.Errorf("result data was not int64")
		}
		for i := 0; i < arr.Len(); i++ {
			if arr.IsNull(i) {
				fmt.Println("theresult: NULL")
			} else {
				fmt.Printf("theresult: %d\n", arr.Value(i))
			}
		}
	}

	return nil
}

func Example() {
	// For this example we will spawn the Flight SQL server ourselves.

	// Create a new database that isn't tied to any other databases that
	// may be in process.
	// db, err := sql.Open("sqlite", "file:example_in_memory?mode=memory")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// defer db.Close()

	// srv, err := sqlite.NewSQLiteFlightSQLServer(db)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// server := flight.NewServerWithMiddleware(nil)
	// server.RegisterFlightService(flightsql.NewFlightServer(srv))
	// err = server.Init("localhost:8080")
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// go func() {
	// 	if err := server.Serve(); err != nil {
	// 		log.Fatal(err)
	// 	}
	// }()

	uri := fmt.Sprintf("grpc://%s", "localhost:32010")
	if err := FlightSQLExample(uri); err != nil {
		log.Printf("Error: %s\n", err.Error())
	}

	// server.Shutdown()

	// Output:
	// theresult: 1
}
