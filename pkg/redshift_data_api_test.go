package rdapp_test

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata/types"
	"github.com/golang/mock/gomock"
	wire "github.com/jeroenrinzema/psql-wire"
	rdapp "github.com/kishaningithub/rdapp/pkg"
	"github.com/kishaningithub/rdapp/pkg/mocks"
	"github.com/lib/pq/oid"
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
	logs                      *observer.ObservedLogs
	config                    rdapp.RedshiftDataAPIConfig
	queryHandler              rdapp.RedshiftDataAPIQueryHandler
}

func TestRedshiftQueryHandlerTestSuite(t *testing.T) {
	suite.Run(t, new(RedshiftQueryHandlerTestSuite))
}

func (suite *RedshiftQueryHandlerTestSuite) SetupTest() {
	suite.mockController = gomock.NewController(suite.T())
	suite.mockRedshiftDataApiClient = mocks.NewMockRedshiftDataApiClient(suite.mockController)
	zapCore, logs := observer.New(zap.DebugLevel)
	suite.logs = logs
	suite.config = rdapp.RedshiftDataAPIConfig{
		Database:          aws.String("database"),
		ClusterIdentifier: aws.String("clusterIdentifier"),
		DbUser:            aws.String("dbUser"),
		SecretArn:         aws.String("secretArn"),
		WorkgroupName:     aws.String("workgroupName"),
	}

	suite.queryHandler = rdapp.NewRedshiftDataApiQueryHandler(suite.mockRedshiftDataApiClient, suite.config, zap.New(zapCore))
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
	loggedEntries := suite.logs.FilterLevelExact(zap.ErrorLevel).TakeAll()
	suite.Require().Len(loggedEntries, 1)
	suite.Require().Equal(zap.ErrorLevel, loggedEntries[0].Level)
	suite.Require().Equal("error while performing execute statement operation", loggedEntries[0].Message)
}

func (suite *RedshiftQueryHandlerTestSuite) TestQueryHandler_ShouldLogAndFailWhenDescribeStatementCallFails() {
	mockDataWriter := mocks.NewMockDataWriter(suite.mockController)
	ctx := context.Background()
	suite.mockRedshiftDataApiClient.EXPECT().ExecuteStatement(gomock.Any(), gomock.Any()).Return(&redshiftdata.ExecuteStatementOutput{
		Id: aws.String("queryId"),
	}, nil)
	cause := fmt.Errorf("call failed")
	suite.mockRedshiftDataApiClient.EXPECT().DescribeStatement(gomock.Any(), gomock.Any()).Return(nil, cause)

	actualErr := suite.queryHandler.QueryHandler(ctx, "queryString", mockDataWriter, nil)

	suite.Require().EqualError(actualErr, "error while performing describe statement operation: call failed")
	loggedEntries := suite.logs.FilterLevelExact(zap.ErrorLevel).TakeAll()
	suite.Require().Len(loggedEntries, 1)
	suite.Require().Equal(zap.ErrorLevel, loggedEntries[0].Level)
	suite.Require().Equal("error while performing describe statement operation", loggedEntries[0].Message)
}

func (suite *RedshiftQueryHandlerTestSuite) TestQueryHandler_ShouldLogAndFailWhenGetStatementResultCallFails() {
	mockDataWriter := mocks.NewMockDataWriter(suite.mockController)
	ctx := context.Background()
	queryId := "queryId"
	suite.mockRedshiftDataApiClient.EXPECT().ExecuteStatement(gomock.Any(), gomock.Any()).Return(&redshiftdata.ExecuteStatementOutput{
		Id: aws.String(queryId),
	}, nil)
	suite.mockRedshiftDataApiClient.EXPECT().DescribeStatement(gomock.Any(), gomock.Any()).Return(&redshiftdata.DescribeStatementOutput{
		Id:           aws.String(queryId),
		HasResultSet: aws.Bool(true),
		Status:       types.StatusStringFinished,
	}, nil)
	cause := fmt.Errorf("call failed")
	suite.mockRedshiftDataApiClient.EXPECT().GetStatementResult(gomock.Any(), gomock.Any()).Return(nil, cause)

	actualErr := suite.queryHandler.QueryHandler(ctx, "queryString", mockDataWriter, nil)

	suite.Require().EqualError(actualErr, "error while getting statement result: call failed")
	loggedEntries := suite.logs.FilterLevelExact(zap.ErrorLevel).TakeAll()
	suite.Require().Len(loggedEntries, 1)
	suite.Require().Equal(zap.ErrorLevel, loggedEntries[0].Level)
	suite.Require().Equal("error while getting statement result", loggedEntries[0].Message)
}

func (suite *RedshiftQueryHandlerTestSuite) TestQueryHandler_ShouldJustWriteOKToWireIfQueryDoesNotHaveAResultSet() {
	mockDataWriter := mocks.NewMockDataWriter(suite.mockController)
	ctx := context.Background()
	queryId := "queryId"
	suite.mockRedshiftDataApiClient.EXPECT().ExecuteStatement(gomock.Any(), gomock.Any()).Return(&redshiftdata.ExecuteStatementOutput{
		Id: aws.String(queryId),
	}, nil)
	suite.mockRedshiftDataApiClient.EXPECT().DescribeStatement(gomock.Any(), gomock.Any()).Return(&redshiftdata.DescribeStatementOutput{
		Id:           aws.String(queryId),
		HasResultSet: aws.Bool(false),
		Status:       types.StatusStringFinished,
	}, nil)
	mockDataWriter.EXPECT().Complete("OK")

	actualErr := suite.queryHandler.QueryHandler(ctx, "queryString", mockDataWriter, nil)

	suite.Require().NoError(actualErr)
}

func (suite *RedshiftQueryHandlerTestSuite) TestQueryHandler_ShouldWriteResultsInWireFormatForAllValueTypesWithColumnMetaData() {
	mockDataWriter := mocks.NewMockDataWriter(suite.mockController)
	ctx := context.Background()
	queryId := "queryId"
	suite.mockRedshiftDataApiClient.EXPECT().ExecuteStatement(gomock.Any(), gomock.Any()).Return(&redshiftdata.ExecuteStatementOutput{
		Id: aws.String(queryId),
	}, nil)
	suite.mockRedshiftDataApiClient.EXPECT().DescribeStatement(gomock.Any(), gomock.Any()).Return(&redshiftdata.DescribeStatementOutput{
		Id:           aws.String(queryId),
		HasResultSet: aws.Bool(true),
		Status:       types.StatusStringFinished,
	}, nil)
	suite.mockRedshiftDataApiClient.EXPECT().GetStatementResult(gomock.Any(), gomock.Any()).Return(&redshiftdata.GetStatementResultOutput{
		Records: [][]types.Field{
			{
				&types.FieldMemberStringValue{Value: "stringValue"},
				&types.FieldMemberBooleanValue{Value: true},
				&types.FieldMemberDoubleValue{Value: 2.44},
				&types.FieldMemberLongValue{Value: 9999},
				&types.FieldMemberIsNull{Value: true},
			},
		},
		ColumnMetadata: []types.ColumnMetadata{
			{Name: aws.String("col1"), TypeName: aws.String(rdapp.RedshiftTypeVarchar)},
			{Name: aws.String("col2"), TypeName: aws.String(rdapp.RedshiftTypeBool)},
			{Name: aws.String("col3"), TypeName: aws.String(rdapp.RedshiftTypeFloat8)},
			{Name: aws.String("col4"), TypeName: aws.String(rdapp.RedshiftTypeInt8)},
			{Name: aws.String("col5"), TypeName: aws.String(rdapp.RedshiftTypeTimestamptz)},
		},
	}, nil)
	mockDataWriter.EXPECT().Define(wire.Columns{
		{Name: "col1", Oid: oid.T_varchar, Width: 0},
		{Name: "col2", Oid: oid.T_bool, Width: 0},
		{Name: "col3", Oid: oid.T_float8, Width: 0},
		{Name: "col4", Oid: oid.T_int8, Width: 0},
		{Name: "col5", Oid: oid.T_timestamptz, Width: 0},
	})
	mockDataWriter.EXPECT().Row([]any{"stringValue", true, 2.44, int64(9999), nil})
	mockDataWriter.EXPECT().Complete("OK")

	actualErr := suite.queryHandler.QueryHandler(ctx, "queryString", mockDataWriter, nil)

	suite.Require().NoError(actualErr)
}
