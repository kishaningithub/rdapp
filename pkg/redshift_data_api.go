package rdapp

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata/types"
	"github.com/google/uuid"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/lib/pq/oid"
	"go.uber.org/zap"
	"strconv"
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

type RedshiftDataAPIQueryHandler interface {
	QueryHandler(ctx context.Context, query string, writer wire.DataWriter, parameters []string) error
}

type RedshiftDataApiClient interface {
	ExecuteStatement(ctx context.Context, params *redshiftdata.ExecuteStatementInput, optFns ...func(*redshiftdata.Options)) (*redshiftdata.ExecuteStatementOutput, error)
	DescribeStatement(ctx context.Context, params *redshiftdata.DescribeStatementInput, optFns ...func(*redshiftdata.Options)) (*redshiftdata.DescribeStatementOutput, error)
	GetStatementResult(ctx context.Context, params *redshiftdata.GetStatementResultInput, optFns ...func(*redshiftdata.Options)) (*redshiftdata.GetStatementResultOutput, error)
}

type redshiftDataApiQueryHandler struct {
	logger                *zap.Logger
	redshiftDataAPIConfig *RedshiftDataAPIConfig
	redshiftDataApiClient RedshiftDataApiClient
}

func NewRedshiftDataApiQueryHandler(redshiftDataApiClient RedshiftDataApiClient, redshiftDataAPIConfig *RedshiftDataAPIConfig, logger *zap.Logger) RedshiftDataAPIQueryHandler {
	return &redshiftDataApiQueryHandler{
		logger:                logger,
		redshiftDataAPIConfig: redshiftDataAPIConfig,
		redshiftDataApiClient: redshiftDataApiClient,
	}
}

func (handler *redshiftDataApiQueryHandler) QueryHandler(ctx context.Context, query string, writer wire.DataWriter, parameters []string) error {
	loggerWithContext := handler.logger.With(
		zap.String("rdappCorrelationId", uuid.NewString()),
	)
	loggerWithContext.Info("received query",
		zap.String("query", query),
		zap.Strings("queryParameters", parameters))
	queryId, err := handler.executeStatement(ctx, query, parameters, loggerWithContext)
	if err != nil {
		return err
	}
	loggerWithContext = loggerWithContext.With(zap.String("redshiftDataApiQueryId", queryId))
	loggerWithContext.Info("submitted query to redshift data api")
	describeStatementOutput, err := handler.waitForQueryToFinish(ctx, queryId, loggerWithContext)
	if err != nil {
		return err
	}
	loggerWithContext = loggerWithContext.With(zap.Int64("redshiftQueryId", describeStatementOutput.RedshiftQueryId))
	loggerWithContext.Info("query finished execution",
		zap.Int64("resultRows", describeStatementOutput.ResultRows),
		zap.Bool("hasResultSet", *describeStatementOutput.HasResultSet),
	)
	if *describeStatementOutput.HasResultSet {
		result, err := handler.redshiftDataApiClient.GetStatementResult(ctx, &redshiftdata.GetStatementResultInput{
			Id: aws.String(queryId),
		})
		if err != nil {
			loggerWithContext.Error("error while getting statement result",
				zap.Error(err),
			)
			return fmt.Errorf("error while getting statement result: %w", err)
		}
		loggerWithContext.Info("received get statement result from redshift",
			zap.Int64("noOfRowsReturned", result.TotalNumRows))
		err = handler.writeResultToWire(result, writer, loggerWithContext)
		if err != nil {
			return err
		}
		loggerWithContext.Info("completed writing result into the wire")
	}
	return writer.Complete("OK")
}

func (handler *redshiftDataApiQueryHandler) writeResultToWire(result *redshiftdata.GetStatementResultOutput, writer wire.DataWriter, loggerWithContext *zap.Logger) error {
	var wireColumns wire.Columns
	for _, column := range result.ColumnMetadata {
		postgresType, err := handler.convertRedshiftResultTypeToPostgresType(*column.TypeName, loggerWithContext)
		if err != nil {
			return err
		}
		wireColumns = append(wireColumns, wire.Column{
			Name:  *column.Name,
			Oid:   postgresType,
			Width: int16(column.Length),
		})
	}
	err := writer.Define(wireColumns)
	if err != nil {
		loggerWithContext.Error("error while writing column definition in result set",
			zap.Error(err),
			zap.Any("columnMetadata", result.ColumnMetadata))
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
			loggerWithContext.Error("error while writing row in result set",
				zap.Error(err),
				zap.Any("recordRow", recordRow),
				zap.Any("columnMetadata", result.ColumnMetadata))
			return fmt.Errorf("error while writing row in result set: %w", err)
		}
	}
	return nil
}

func (handler *redshiftDataApiQueryHandler) convertRedshiftResultTypeToPostgresType(redshiftTypeName string, loggerWithContext *zap.Logger) (oid.Oid, error) {
	typeConversions := map[string]oid.Oid{
		"super": oid.T_json,
		"bool":  oid.T_bool,
		// Character types
		"char":    oid.T_varchar,
		"varchar": oid.T_varchar,
		"bpchar":  oid.T_bpchar,
		// Timestamp types
		"timestamp":   oid.T_timestamp,
		"timestamptz": oid.T_timestamptz,
		// Numeric types
		"float4": oid.T_float4,
		"float8": oid.T_float8,
		"int2":   oid.T_int2,
		"int4":   oid.T_int4,
		"int8":   oid.T_int8,
		//	Esoteric types
		"name":     oid.T_name,
		"oid":      oid.T_oid,
		"_aclitem": oid.T_aclitem,
	}
	value, exists := typeConversions[redshiftTypeName]
	if !exists {
		loggerWithContext.Error("no convertor found for redshift type",
			zap.String("redshiftTypeName", redshiftTypeName))
		return 0, fmt.Errorf("no convertor found redshiftTypeName=%v", redshiftTypeName)
	}
	return value, nil
}

func (handler *redshiftDataApiQueryHandler) executeStatement(ctx context.Context, query string, parameters []string, loggerWithContext *zap.Logger) (string, error) {
	var sqlParameters []types.SqlParameter
	for i, parameter := range parameters {
		sqlParameters = append(sqlParameters, types.SqlParameter{
			Name:  aws.String(strconv.Itoa(i + 1)),
			Value: aws.String(parameter),
		})
	}
	output, err := handler.redshiftDataApiClient.ExecuteStatement(ctx, &redshiftdata.ExecuteStatementInput{
		Database:          handler.redshiftDataAPIConfig.Database,
		Sql:               aws.String(strings.ReplaceAll(query, "$", ":")),
		ClusterIdentifier: handler.redshiftDataAPIConfig.ClusterIdentifier,
		DbUser:            handler.redshiftDataAPIConfig.DbUser,
		SecretArn:         handler.redshiftDataAPIConfig.SecretArn,
		StatementName:     aws.String("execute_rdapp_query"),
		WithEvent:         aws.Bool(true),
		WorkgroupName:     handler.redshiftDataAPIConfig.WorkgroupName,
		Parameters:        sqlParameters,
	})
	if err != nil {
		loggerWithContext.Error("error while performing execute statement operation",
			zap.Error(err))
		return "", fmt.Errorf("error while performing execute statement operation: %w", err)
	}
	queryId := *output.Id
	return queryId, nil
}

func (handler *redshiftDataApiQueryHandler) waitForQueryToFinish(ctx context.Context, queryId string, loggerWithContext *zap.Logger) (*redshiftdata.DescribeStatementOutput, error) {
	for {
		result, err := handler.redshiftDataApiClient.DescribeStatement(ctx, &redshiftdata.DescribeStatementInput{
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
