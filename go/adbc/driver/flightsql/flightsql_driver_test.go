package flightsql_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/flightsql"
	fsql "github.com/apache/arrow/go/v11/arrow/flight/flightsql"
	"github.com/apache/arrow/go/v11/arrow/memory"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type DremioTestSuite struct {
	suite.Suite

	user, pass string
	loc        string

	alloc *memory.CheckedAllocator
	db    adbc.Database
	ctx   context.Context
}

var drv = flightsql.Driver{}

func (d *DremioTestSuite) SetupTest() {
	d.ctx = context.Background()
	d.alloc = memory.NewCheckedAllocator(memory.DefaultAllocator)
	drv.Alloc = d.alloc
	var err error
	d.db, err = drv.NewDatabase(map[string]string{
		adbc.OptionKeyUsername: d.user,
		adbc.OptionKeyPassword: d.pass,
		adbc.OptionKeyURI:      d.loc,
	})
	d.Require().NoError(err)
}

func (d *DremioTestSuite) TearDownTest() {
	d.db = nil
	d.alloc.AssertSize(d.T(), 0)
}

func (d *DremioTestSuite) TestGetTableSchemas() {
	conn, err := d.db.Open(d.ctx)
	d.Require().NoError(err)
	defer conn.Close()

	var schema = "Samples.samples.dremio.com"
	sc, err := conn.GetTableSchema(d.ctx, nil, &schema, "NYC-taxi-trips-iceberg")
	d.NoError(err)
	fmt.Println(sc)
}

func (d *DremioTestSuite) TestGetSqlInfo() {
	conn, err := d.db.Open(d.ctx)
	d.Require().NoError(err)
	defer conn.Close()

	info := []adbc.InfoCode{adbc.InfoVendorName}
	rdr, err := conn.GetInfo(d.ctx, info)
	d.Require().NoError(err)
	defer rdr.Release()
}

func TestFlightSQLDremio(t *testing.T) {
	suite.Run(t, &DremioTestSuite{user: "dremio", pass: "dremio123", loc: "grpc+tcp://localhost:32010"})
}

func TestDremioFlightSQLDirect(t *testing.T) {
	cl, err := fsql.NewClient("localhost:32010", nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer cl.Close()

	ctx, err := cl.Client.AuthenticateBasicToken(context.Background(), "dremio", "dremio123")
	require.NoError(t, err)
	// info, err := cl.GetTables(ctx, &fsql.GetTablesOpts{TableTypes: []string{"TABLE"}})

	info, err := cl.GetSqlInfo(ctx, []fsql.SqlInfo{fsql.SqlInfoKeywords})
	require.NoError(t, err)
	rdr, err := cl.DoGet(ctx, info.Endpoint[0].Ticket)
	require.NoError(t, err)
	defer rdr.Release()
}
