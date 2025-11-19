package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow-adbc/go/adbc/sqldriver"
)

var drvname = "duckdb"
var dbpath = "my.duckdb"

func test_sqldriver() {
	fmt.Println("Running test with sqldriver...")

	dsn := fmt.Sprintf("driver=%s;path=%s", drvname, dbpath)

	var drv drivermgr.Driver
	sql.Register("test", &sqldriver.Driver{Driver: &drv})

	db, err := sql.Open("test", dsn)
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, "CREATE TABLE IF NOT EXISTS example(id INTEGER);")
	if err != nil {
		panic(err)
	}
	rows.Close()

	// Manually close db
	if err := db.Close(); err != nil {
		panic(err)
	}

	fmt.Println("Ctrl+C exits")
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
}

// func test_drivermgr() {
// 	fmt.Println("Running test with drivermgr directly...")

// 	var drv drivermgr.Driver
// 	db, err := drv.NewDatabase(map[string]string{"driver": drvname, "path": dbpath})
// 	if err != nil {
// 		panic(err)
// 	}
// 	conn, err := db.Open(context.Background())
// 	if err != nil {
// 		panic(err)
// 	}

// 	stmt, err := conn.NewStatement()
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	err = stmt.SetSqlQuery("CREATE TABLE IF NOT EXISTS example(id INTEGER);")
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	stream, _, err := stmt.ExecuteQuery(context.Background())
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	defer stream.Release()

// 	// Manually close things instead of defer so we can reproduce the issue
// 	err = stmt.Close()
// 	if err != nil {
// 		panic(err)
// 	}
// 	err = conn.Close()
// 	if err != nil {
// 		panic(err)
// 	}
// 	err = db.Close()
// 	if err != nil {
// 		panic(err)
// 	}

// 	fmt.Println("Ctrl+C exits")
// 	sig := make(chan os.Signal, 1)
// 	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
// 	<-sig
// }

func main() {
	// The issue can be reproduce when this is run:
	test_sqldriver()
	// and I ran uv run --with "duckdb" python -c "import duckdb; duckdb.connect(database='my.duckdb')"

	// With the code in this function, I can't reproduce the issue
	// test_drivermgr()
}
