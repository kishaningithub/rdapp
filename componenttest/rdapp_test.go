package componenttest

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/jackc/pgx/v4"
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
	listenAddress := ":35432"
	logger := zap.NewExample()
	cfg, err := config.LoadDefaultConfig(context.Background())
	suite.Require().NoError(err)
	redshiftDataAPIConfig := rdapp.RedshiftDataAPIConfig{
		Database:      aws.String("dev"),
		WorkgroupName: aws.String("rdapp"),
	}
	proxy := rdapp.ConstructProxy(cfg, redshiftDataAPIConfig, logger, listenAddress)
	go func() {
		logger.Info("Starting test instance of postgres redshift proxy...")
		err := proxy.Run()
		suite.Require().NoError(err)
	}()
	databaseUrl := "postgres://postgres:mypassword@localhost:35432/postgres"
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
