package rdapp

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	wire "github.com/jeroenrinzema/psql-wire"
	"go.uber.org/zap"
)

type RedshiftDataApiQueryHandler interface {
	QueryHandler(ctx context.Context, query string, writer wire.DataWriter, parameters []string) error
}

type redshiftDataApiQueryHandler struct {
	redshiftDataAPIService RedshiftDataAPIService
	pgRedshiftTranslator   PgRedshiftTranslator
	logger                 *zap.Logger
}

func NewRedshiftDataApiQueryHandler(redshiftDataAPIService RedshiftDataAPIService, pgRedshiftTranslator PgRedshiftTranslator, logger *zap.Logger) RedshiftDataApiQueryHandler {
	return &redshiftDataApiQueryHandler{
		redshiftDataAPIService: redshiftDataAPIService,
		pgRedshiftTranslator:   pgRedshiftTranslator,
		logger:                 logger,
	}
}

func (handler *redshiftDataApiQueryHandler) QueryHandler(ctx context.Context, query string, writer wire.DataWriter, parameters []string) error {
	rdappCtx := RdappContext{
		Context: ctx,
		logger: handler.logger.With(
			zap.String("rdappCorrelationId", uuid.NewString()),
		),
	}
	loggerWithContext := rdappCtx.logger
	loggerWithContext.Info("received query",
		zap.String("query", query),
		zap.Any("parameters", parameters))
	redshiftQuery := handler.pgRedshiftTranslator.TranslateToRedshiftQuery(query)
	redshiftQueryParams := handler.pgRedshiftTranslator.TranslateToRedshiftQueryParams(parameters)
	result, err := handler.redshiftDataAPIService.ExecuteQuery(rdappCtx, redshiftQuery, redshiftQueryParams)
	if err != nil {
		return err
	}
	if result != nil {
		pgColumnMetaData, err := handler.pgRedshiftTranslator.TranslateColumnMetaDataToPgFormat(rdappCtx, result.ColumnMetadata)
		if err != nil {
			return err
		}
		err = writer.Define(pgColumnMetaData)
		if err != nil {
			loggerWithContext.Error("error while writing column definition in result set",
				zap.Error(err),
				zap.Any("columnMetadata", result.ColumnMetadata))
			return err
		}
		for _, redshiftRow := range result.Records {
			row, err := handler.pgRedshiftTranslator.TranslateRowToPgFormat(rdappCtx, redshiftRow)
			if err != nil {
				return err
			}
			err = writer.Row(row)
			if err != nil {
				rdappCtx.logger.Error("error while writing row in redshiftFields set",
					zap.Error(err),
					zap.Any("recordRow", redshiftRow),
					zap.Any("columnMetadata", result.ColumnMetadata))
				return fmt.Errorf("error while writing row in redshiftFields set: %w", err)
			}
		}
		loggerWithContext.Info("completed writing result into the wire")
	}
	return writer.Complete("OK")
}
