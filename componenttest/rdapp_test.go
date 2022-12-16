package componenttest

import (
	"context"
	"database/sql"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	_ "github.com/jackc/pgx/v5/stdlib"
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
	conn *sql.DB
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
	conn, err := sql.Open("pgx", databaseUrl)
	suite.Require().NoError(err)
	suite.conn = conn
}

func (suite *RedshiftDataAPIProxyTestSuite) TearDownSuite() {
	_ = suite.conn.Close()
}

func (suite *RedshiftDataAPIProxyTestSuite) TestConnectivity() {
	err := suite.conn.Ping()
	suite.Require().NoError(err)
}

func (suite *RedshiftDataAPIProxyTestSuite) TestSimpleQueryExecution() {
	var intValue int
	var stringValue string
	var boolValue bool
	var timeStamp time.Time
	rows, err := suite.conn.Query("select 1, 'name', true, now()")
	defer rows.Close()
	suite.Require().NoError(err)
	suite.Require().True(rows.Next())
	err = rows.Scan(&intValue, &stringValue, &boolValue, &timeStamp)
	suite.Require().NoError(err)
	suite.Require().Equal(1, intValue)
	suite.Require().Equal("name", stringValue)
	suite.Require().Equal(true, boolValue)
	suite.Require().WithinDuration(time.Now().UTC(), timeStamp, 10*time.Second)
}

func (suite *RedshiftDataAPIProxyTestSuite) TestPreparedStatementQueryExecution() {
	query := `
      select * 
      from (
		  select 1 id
		  union all
		  select 2 id
		  union all
		  select 3 id
      )
      where id > $1
   `
	stmt, err := suite.conn.Prepare(query)
	suite.Require().NoError(err)
	defer stmt.Close()
	rows, err := stmt.Query(1)
	suite.Require().NoError(err)
	defer rows.Close()
	var ids []int
	for rows.Next() {
		var id int
		err := rows.Scan(&id)
		suite.Require().NoError(err)
		ids = append(ids, id)
	}
	suite.Require().NoError(rows.Err())
	suite.Require().Equal([]int{2, 3}, ids)
}
