package rdapp

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata/types"
	"go.uber.org/zap"
	"strings"
)

type RedshiftDataAPIConfig struct {
	// The name of the database. This parameter is required when authenticating using
	// either Secrets Manager or temporary credentials.
	//
	// This member is required.
	Database *string

	// The cluster identifier. This parameter is required when connecting to a cluster
	// and authenticating using either Secrets Manager or temporary credentials.
	ClusterIdentifier *string

	// The database user name. This parameter is required when connecting to a cluster
	// and authenticating using temporary credentials.
	DbUser *string

	// The name or ARN of the secret that enables access to the database. This
	// parameter is required when authenticating using Secrets Manager.
	SecretArn *string

	// The serverless workgroup name. This parameter is required when connecting to a
	// serverless workgroup and authenticating using either Secrets Manager or
	// temporary credentials.
	WorkgroupName *string
}

type RedshiftDataAPIService interface {
	ExecuteQuery(ctx RdappContext, query string, parameters []types.SqlParameter) (*redshiftdata.GetStatementResultOutput, error)
}

type redshiftDataAPIService struct {
	redshiftDataAPIConfig RedshiftDataAPIConfig
	redshiftDataApiClient RedshiftDataApiClient
}

func NewRedshiftDataAPIService(redshiftDataApiClient RedshiftDataApiClient, redshiftDataAPIConfig RedshiftDataAPIConfig) RedshiftDataAPIService {
	return &redshiftDataAPIService{
		redshiftDataAPIConfig: redshiftDataAPIConfig,
		redshiftDataApiClient: redshiftDataApiClient,
	}
}

func (service *redshiftDataAPIService) ExecuteQuery(ctx RdappContext, query string, parameters []types.SqlParameter) (*redshiftdata.GetStatementResultOutput, error) {
	loggerWithContext := ctx.logger
	if strings.Contains(query, "deallocate") {
		return nil, nil
	}
	queryId, err := service.executeStatement(ctx, query, parameters, loggerWithContext)
	if err != nil {
		return nil, err
	}
	loggerWithContext = loggerWithContext.With(zap.String("redshiftDataApiQueryId", queryId))
	loggerWithContext.Info("submitted query to redshift data api")
	describeStatementOutput, err := service.waitForQueryToFinish(ctx, queryId, loggerWithContext)
	if err != nil {
		return nil, err
	}
	loggerWithContext = loggerWithContext.With(zap.Int64("redshiftQueryId", describeStatementOutput.RedshiftQueryId))
	loggerWithContext.Info("query finished execution",
		zap.Int64("resultRows", describeStatementOutput.ResultRows),
		zap.Bool("hasResultSet", *describeStatementOutput.HasResultSet),
	)
	if *describeStatementOutput.HasResultSet {
		result, err := service.redshiftDataApiClient.GetStatementResult(ctx, &redshiftdata.GetStatementResultInput{
			Id: aws.String(queryId),
		})
		if err != nil {
			loggerWithContext.Error("error while getting statement result",
				zap.Error(err),
			)
			return nil, fmt.Errorf("error while getting statement result: %w", err)
		}
		loggerWithContext.Info("received get statement result from redshift",
			zap.Int64("noOfRowsReturned", result.TotalNumRows))
		return result, nil
	}
	return nil, nil
}

func (service *redshiftDataAPIService) executeStatement(ctx context.Context, query string, parameters []types.SqlParameter, loggerWithContext *zap.Logger) (string, error) {
	loggerWithContext.Info("executing query",
		zap.String("query", query),
		zap.Any("parameters", parameters))
	output, err := service.redshiftDataApiClient.ExecuteStatement(ctx, &redshiftdata.ExecuteStatementInput{
		Database:          service.redshiftDataAPIConfig.Database,
		Sql:               aws.String(query),
		ClusterIdentifier: service.redshiftDataAPIConfig.ClusterIdentifier,
		DbUser:            service.redshiftDataAPIConfig.DbUser,
		SecretArn:         service.redshiftDataAPIConfig.SecretArn,
		StatementName:     aws.String("execute_rdapp_query"),
		WithEvent:         aws.Bool(true),
		WorkgroupName:     service.redshiftDataAPIConfig.WorkgroupName,
		Parameters:        parameters,
	})
	if err != nil {
		loggerWithContext.Error("error while performing execute statement operation",
			zap.Error(err))
		return "", fmt.Errorf("error while performing execute statement operation: %w", err)
	}
	queryId := *output.Id
	return queryId, nil
}

func (service *redshiftDataAPIService) waitForQueryToFinish(ctx context.Context, queryId string, loggerWithContext *zap.Logger) (*redshiftdata.DescribeStatementOutput, error) {
	for {
		result, err := service.redshiftDataApiClient.DescribeStatement(ctx, &redshiftdata.DescribeStatementInput{
			Id: aws.String(queryId),
		})
		if err != nil {
			loggerWithContext.Error("error while performing describe statement operation",
				zap.Error(err))
			return nil, fmt.Errorf("error while performing describe statement operation: %w", err)
		}
		switch result.Status {
		case types.StatusStringFinished:
			return result, nil
		case types.StatusStringAborted, types.StatusStringFailed:
			err := fmt.Errorf(*result.Error)
			loggerWithContext.Error("query execution failed or aborted",
				zap.String("redshiftDataApiQueryId", queryId),
				zap.Error(err),
				zap.Int64("redshiftQueryId", result.RedshiftQueryId))
			return nil, fmt.Errorf("query execution failed or aborted: %w", err)
		default:
			loggerWithContext.Debug("received query status",
				zap.String("queryStatus", string(result.Status)))
		}
	}
}
