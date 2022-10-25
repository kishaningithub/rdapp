package rdapp

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
	wire "github.com/jeroenrinzema/psql-wire"
	"go.uber.org/zap"
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

type RedshiftDataAPIQueryHandler interface {
	QueryHandler(ctx context.Context, query string, writer wire.DataWriter, parameters []string) error
}

type redshiftDataApiQueryHandler struct {
	logger                *zap.Logger
	redshiftDataAPIConfig *RedshiftDataAPIConfig
	redshiftDataApiClient *redshiftdata.Client
}

func NewRedshiftDataApiQueryHandler(redshiftDataApiClient *redshiftdata.Client, redshiftDataAPIConfig *RedshiftDataAPIConfig, logger *zap.Logger) RedshiftDataAPIQueryHandler {
	return &redshiftDataApiQueryHandler{
		logger:                logger,
		redshiftDataAPIConfig: redshiftDataAPIConfig,
		redshiftDataApiClient: redshiftDataApiClient,
	}
}

func (handler *redshiftDataApiQueryHandler) QueryHandler(ctx context.Context, query string, writer wire.DataWriter, parameters []string) error {
	handler.logger.Info("received query", zap.String("query", query), zap.Strings("parameters", parameters))
	output, err := handler.redshiftDataApiClient.ExecuteStatement(context.Background(), &redshiftdata.ExecuteStatementInput{
		Database:          handler.redshiftDataAPIConfig.Database,
		Sql:               aws.String(query),
		ClusterIdentifier: handler.redshiftDataAPIConfig.ClusterIdentifier,
		DbUser:            handler.redshiftDataAPIConfig.DbUser,
		SecretArn:         handler.redshiftDataAPIConfig.SecretArn,
		StatementName:     aws.String("execute_rdapp_query"),
		WithEvent:         aws.Bool(true),
		WorkgroupName:     handler.redshiftDataAPIConfig.WorkgroupName,
	})
	if err != nil {
		return err
	}
	queryId := *output.Id
	handler.logger.Info("completed execute statement call", zap.String("queryId", queryId))
	return writer.Complete("OK")
}
