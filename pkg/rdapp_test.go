package rdapp_test

import (
	"context"
	"github.com/jackc/pgx/v4"
	wire "github.com/jeroenrinzema/psql-wire"
	rdapp "github.com/kishaningithub/rdapp/pkg"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"testing"
)

var (
	_ suite.SetupAllSuite    = (*RedshiftDataAPIProxyTestSuite)(nil)
	_ suite.TearDownAllSuite = (*RedshiftDataAPIProxyTestSuite)(nil)
)

type RedshiftDataAPIProxyTestSuite struct {
	suite.Suite
	conn *pgx.Conn
}

func TestRedshiftDataAPIProxyTestSuite(t *testing.T) {
	suite.Run(t, new(RedshiftDataAPIProxyTestSuite))
}

func (suite *RedshiftDataAPIProxyTestSuite) SetupSuite() {
	listenAddress := "localhost:25432"
	logger := zap.NewExample()
	go func() {
		logger.Info("Starting test instance of postgres redshift proxy...")
		err := rdapp.NewPostgresRedshiftDataAPIProxy(listenAddress, func(ctx context.Context, query string, writer wire.DataWriter, parameters []string) error {
			return writer.Complete("OK")
		}, logger).Run()
		suite.Require().NoError(err)
	}()
	databaseUrl := "postgres://postgres:mypassword@localhost:25432/postgres"
	conn, err := pgx.Connect(context.Background(), databaseUrl)
	suite.Require().NoError(err)
	suite.conn = conn
}

func (suite *RedshiftDataAPIProxyTestSuite) TearDownSuite() {
	_ = suite.conn.Close(context.Background())
}

func (suite *RedshiftDataAPIProxyTestSuite) TestConnectivity() {
	connInfo := suite.conn.Config()
	suite.Require().Equal("localhost", connInfo.Host)
}

func (suite *RedshiftDataAPIProxyTestSuite) TestQueryExecution() {
	rows, err := suite.conn.Query(context.Background(), "select name, weight from widgets limit 1")
	suite.Require().NoError(err)
	suite.Require().NoError(rows.Err())
}
