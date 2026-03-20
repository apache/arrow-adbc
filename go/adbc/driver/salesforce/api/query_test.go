package api

import (
	"context"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc/driver/salesforce/api/types"
	"github.com/stretchr/testify/suite"
)

type QuerySuite struct {
	APISuite
}

func (s *QuerySuite) TestExecuteSqlQuery() {
	// First get a valid entity name from metadata
	meta, err := s.Client.GetMetadata(context.Background(), &types.MetadataRequest{})
	s.Require().NoError(err)
	s.Require().NotEmpty(meta.Metadata, "need at least one entity to query")

	entity := meta.Metadata[0]
	resp, err := s.Client.CreateSqlQuery(context.Background(), &types.SqlQueryRequest{
		SQL:      "SELECT * FROM " + entity.Name + " LIMIT 1",
		RowLimit: 1,
	}, nil)
	s.Require().NoError(err)
	s.NotNil(resp)
	s.NotEmpty(resp.Metadata, "expected column metadata")
}

func TestQuerySuite(t *testing.T) {
	suite.Run(t, new(QuerySuite))
}
