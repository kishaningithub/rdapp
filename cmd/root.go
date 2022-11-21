package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/redshift"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
	"github.com/aws/aws-sdk-go-v2/service/redshiftserverless"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/kishaningithub/rdapp/pkg"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"strings"
)

var Version string

var listenAddress string
var dbUser string
var clusterIdentifier string
var database string
var secretArn string
var workgroupName string
var verboseLogging bool

var rootCmd = &cobra.Command{
	Use:     "rdapp",
	Short:   "rdapp - Redshift Data API Postgres Proxy",
	Long:    `Use your favourite postgres tools to query redshift via redshift data api`,
	RunE:    runRootCommand,
	Version: Version,
}

func init() {
	rootCmd.Flags().StringVar(&listenAddress, "listen", "127.0.0.1:25432", "")
	rootCmd.Flags().StringVar(&clusterIdentifier, "cluster-identifier", "", "")
	rootCmd.Flags().StringVar(&database, "database", "", "")
	rootCmd.Flags().StringVar(&dbUser, "db-user", "", "")
	rootCmd.Flags().StringVar(&secretArn, "secret-arn", "", "")
	rootCmd.Flags().StringVar(&workgroupName, "workgroup-name", "", "")
	rootCmd.Flags().BoolVar(&verboseLogging, "verbose", false, "verbose output")
}

func main() {
	_ = rootCmd.Execute()
}

func runRootCommand(_ *cobra.Command, _ []string) error {
	logger := constructLogger()
	defer func(logger *zap.Logger) {
		_ = logger.Sync()
	}(logger)
	rootContext := context.Background()
	cfg, err := config.LoadDefaultConfig(rootContext)
	if err != nil {
		return fmt.Errorf("error while loading aws config: %w", err)
	}
	redshiftDataApiClient := redshiftdata.NewFromConfig(cfg)
	redshiftDataApiConfig := rdapp.RedshiftDataAPIConfig{
		Database:          getFlagValue(database),
		ClusterIdentifier: getFlagValue(clusterIdentifier),
		DbUser:            getFlagValue(dbUser),
		SecretArn:         getFlagValue(secretArn),
		WorkgroupName:     getFlagValue(workgroupName),
	}
	if redshiftDataApiConfig.Database == nil {
		fmt.Println("Loading interactive config setup view...")
		redshiftClient := redshift.NewFromConfig(cfg)
		redshiftServerlessClient := redshiftserverless.NewFromConfig(cfg)
		secretsManagerClient := secretsmanager.NewFromConfig(cfg)
		redshiftService := rdapp.NewRedshiftService(redshiftClient, redshiftServerlessClient)
		secretsService := rdapp.NewSecretsService(secretsManagerClient)
		service := NewInteractionService(redshiftService, secretsService)
		redshiftDataApiConfig, err = service.Interact(rootContext)
		if err != nil {
			return err
		}
		logger.Info("using config", zap.Any("config", redshiftDataApiConfig))
	}
	redshiftDataAPIService := rdapp.NewRedshiftDataAPIService(redshiftDataApiClient, redshiftDataApiConfig)
	pgRedshiftTranslator := rdapp.NewPgRedshiftTranslator()
	redshiftDataApiQueryHandler := rdapp.NewRedshiftDataApiQueryHandler(redshiftDataAPIService, pgRedshiftTranslator, logger)
	err = rdapp.NewPostgresRedshiftDataAPIProxy(listenAddress, redshiftDataApiQueryHandler.QueryHandler, logger).Run()
	if err != nil {
		return fmt.Errorf("error while creating postgres redshift proxy: %w", err)
	}
	return nil
}

func getFlagValue(value string) *string {
	if value == "" {
		return nil
	}
	value = strings.TrimSpace(value)
	return &value
}

func constructLogger() *zap.Logger {
	productionConfig := zap.NewProductionConfig()
	productionConfig.EncoderConfig.TimeKey = "timestamp"
	productionConfig.EncoderConfig.EncodeTime = zapcore.RFC3339NanoTimeEncoder
	productionConfig.Level = zap.NewAtomicLevelAt(logLevel())
	productionConfig.DisableStacktrace = true
	logger, _ := productionConfig.Build()
	return logger
}

func logLevel() zapcore.Level {
	if verboseLogging {
		return zap.DebugLevel
	}
	return zap.InfoLevel
}
