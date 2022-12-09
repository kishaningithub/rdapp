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
	"time"
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
	logger, err := zap.NewDevelopment()
	suite.Require().NoError(err)
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
	var intValue int
	var stringValue string
	var boolValue bool
	var timeStamp time.Time
	err := suite.conn.QueryRow(context.Background(), "select 1, 'name', true, now()").
		Scan(&intValue, &stringValue, &boolValue, &timeStamp)
	suite.Require().NoError(err)
	suite.Require().Equal(1, intValue)
	suite.Require().Equal("name", stringValue)
	suite.Require().Equal(true, boolValue)
	suite.Require().WithinDuration(time.Now().UTC(), timeStamp, 10*time.Second)
}
