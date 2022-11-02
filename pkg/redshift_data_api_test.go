package rdapp_test

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/golang/mock/gomock"
	rdapp "github.com/kishaningithub/rdapp/pkg"
	"github.com/kishaningithub/rdapp/pkg/mocks"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"testing"
)

var (
	_ suite.SetupTestSuite    = (*RedshiftQueryHandlerTestSuite)(nil)
	_ suite.TearDownTestSuite = (*RedshiftQueryHandlerTestSuite)(nil)
)

type RedshiftQueryHandlerTestSuite struct {
	suite.Suite
	mockController            *gomock.Controller
	mockRedshiftDataApiClient *mocks.MockRedshiftDataApiClient
	observedLogs              *observer.ObservedLogs
	config                    rdapp.RedshiftDataAPIConfig
	queryHandler              rdapp.RedshiftDataAPIQueryHandler
}

func TestRedshiftQueryHandlerTestSuite(t *testing.T) {
	suite.Run(t, new(RedshiftQueryHandlerTestSuite))
}

func (suite *RedshiftQueryHandlerTestSuite) SetupTest() {
	suite.mockController = gomock.NewController(suite.T())
	suite.mockRedshiftDataApiClient = mocks.NewMockRedshiftDataApiClient(suite.mockController)
	observedZapCore, observedLogs := observer.New(zap.DebugLevel)
	suite.observedLogs = observedLogs
	suite.config = rdapp.RedshiftDataAPIConfig{
		Database:          aws.String("database"),
		ClusterIdentifier: aws.String("clusterIdentifier"),
		DbUser:            aws.String("dbUser"),
		SecretArn:         aws.String("secretArn"),
		WorkgroupName:     aws.String("workgroupName"),
	}

	suite.queryHandler = rdapp.NewRedshiftDataApiQueryHandler(suite.mockRedshiftDataApiClient, &suite.config, zap.New(observedZapCore))
}

func (suite *RedshiftQueryHandlerTestSuite) TearDownTest() {
	suite.mockController.Finish()
}

func (suite *RedshiftQueryHandlerTestSuite) TestQueryHandler_ShouldLogAndFailWhenExecuteStatementCallFails() {
	mockDataWriter := mocks.NewMockDataWriter(suite.mockController)
	ctx := context.Background()
	cause := fmt.Errorf("call failed")
	suite.mockRedshiftDataApiClient.EXPECT().ExecuteStatement(gomock.Any(), gomock.Any()).Return(nil, cause)

	actualErr := suite.queryHandler.QueryHandler(ctx, "queryString", mockDataWriter, nil)

	suite.Require().EqualError(actualErr, "error while performing execute statement operation: call failed")
	loggedEntries := suite.observedLogs.FilterLevelExact(zap.ErrorLevel).TakeAll()
	suite.Require().Len(loggedEntries, 1)
	suite.Require().Equal(zap.ErrorLevel, loggedEntries[0].Level)
	suite.Require().Equal("error while performing execute statement operation", loggedEntries[0].Message)
}
