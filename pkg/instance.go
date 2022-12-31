package rdapp

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
	"go.uber.org/zap"
)

func ConstructProxy(cfg aws.Config, redshiftDataApiConfig RedshiftDataAPIConfig, logger *zap.Logger, listenAddress string) PostgresRedshiftProxy {
	redshiftDataApiClient := redshiftdata.NewFromConfig(cfg)
	redshiftDataAPIService := NewRedshiftDataAPIService(redshiftDataApiClient, redshiftDataApiConfig)
	pgRedshiftTranslator := NewPgRedshiftTranslator()
	redshiftDataApiQueryHandler := NewRedshiftDataApiQueryHandler(redshiftDataAPIService, pgRedshiftTranslator, logger)
	proxy := NewPostgresRedshiftDataAPIProxy(listenAddress, redshiftDataApiQueryHandler.QueryHandler, logger)
	return proxy
}
