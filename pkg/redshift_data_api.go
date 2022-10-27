package rdapp

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata/types"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/lib/pq/oid"
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
	queryId, err := handler.executeStatement(ctx, query)
	if err != nil {
		return err
	}
	describeStatementOutput, err := handler.waitForQueryToFinish(ctx, queryId)
	if err != nil {
		return err
	}
	if *describeStatementOutput.HasResultSet {
		result, err := handler.redshiftDataApiClient.GetStatementResult(ctx, &redshiftdata.GetStatementResultInput{
			Id: aws.String(queryId),
		})
		if err != nil {
			return err
		}
		handler.logger.Debug("received get statement result from redshift", zap.Any("getStatementResult", result))
		var wireColumns wire.Columns
		for _, column := range result.ColumnMetadata {
			postgresType, err := handler.convertRedshiftResultTypeToPostgresType(*column.TypeName)
			if err != nil {
				return err
			}
			wireColumns = append(wireColumns, wire.Column{
				Name:  *column.Name,
				Oid:   postgresType,
				Width: int16(column.Length),
			})
		}
		err = writer.Define(wireColumns)
		if err != nil {
			handler.logger.Error("error while writing column definition in result set",
				zap.String("queryId", queryId),
				zap.Error(err), zap.Any("columnMetadata", result.ColumnMetadata))
			return err
		}
		for _, recordRow := range result.Records {
			var row []any
			for _, recordCol := range recordRow {
				switch t := recordCol.(type) {
				case *types.FieldMemberIsNull:
					row = append(row, nil)
				case *types.FieldMemberBlobValue:
					row = append(row, t.Value)
				case *types.FieldMemberBooleanValue:
					row = append(row, t.Value)
				case *types.FieldMemberDoubleValue:
					row = append(row, t.Value)
				case *types.FieldMemberLongValue:
					row = append(row, t.Value)
				case *types.FieldMemberStringValue:
					row = append(row, t.Value)
				}
			}
			err = writer.Row(row)
			if err != nil {
				handler.logger.Error("error while writing row in result set",
					zap.String("queryId", queryId),
					zap.Error(err), zap.Any("recordRow", recordRow))
				return err
			}
		}
	}
	return writer.Complete("OK")
}

func (handler *redshiftDataApiQueryHandler) convertRedshiftResultTypeToPostgresType(redshiftTypeName string) (oid.Oid, error) {
	switch redshiftTypeName {
	case "varchar":
		return oid.T_varchar, nil
	case "timestamptz":
		return oid.T_timestamptz, nil
	case "float4":
		return oid.T_float4, nil
	case "float8":
		return oid.T_float8, nil
	case "int2":
		return oid.T_int2, nil
	case "int4":
		return oid.T_int4, nil
	case "int8":
		return oid.T_int8, nil
	case "super":
		return oid.T_json, nil
	default:
		return 0, fmt.Errorf("no convertor found redshiftTypeName=%v", redshiftTypeName)
	}
}

func (handler *redshiftDataApiQueryHandler) executeStatement(ctx context.Context, query string) (string, error) {
	output, err := handler.redshiftDataApiClient.ExecuteStatement(ctx, &redshiftdata.ExecuteStatementInput{
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
		return "", err
	}
	queryId := *output.Id
	handler.logger.Info("completed execute statement call", zap.String("queryId", queryId))
	return queryId, nil
}

func (handler *redshiftDataApiQueryHandler) waitForQueryToFinish(ctx context.Context, queryId string) (*redshiftdata.DescribeStatementOutput, error) {
	for {
		result, err := handler.redshiftDataApiClient.DescribeStatement(ctx, &redshiftdata.DescribeStatementInput{
			Id: aws.String(queryId),
		})
		if err != nil {
			return nil, err
		}
		switch result.Status {
		case types.StatusStringFinished:
			handler.logger.Info("query finished execution",
				zap.String("queryId", queryId),
				zap.Int64("redshiftQueryId", result.RedshiftQueryId))
			return result, nil
		case types.StatusStringAborted:
			handler.logger.Error("query aborted",
				zap.String("queryId", queryId),
				zap.Error(fmt.Errorf(*result.Error)),
				zap.Int64("redshiftQueryId", result.RedshiftQueryId))
			return nil, fmt.Errorf("query aborted")
		case types.StatusStringFailed:
			handler.logger.Error("query failed",
				zap.String("queryId", queryId),
				zap.Error(fmt.Errorf(*result.Error)),
				zap.Int64("redshiftQueryId", result.RedshiftQueryId),
				zap.String("query", *result.QueryString))
			return nil, fmt.Errorf("query failed")
		default:
			handler.logger.Debug("query status", zap.String("queryStatus", string(result.Status)))
		}
	}
}