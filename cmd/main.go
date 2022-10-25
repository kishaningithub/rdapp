package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
	"github.com/kishaningithub/rdapp/pkg"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
)

func main() {
	var listenAddress string
	var dbUser string
	var clusterIdentifier string
	var rootCmd = &cobra.Command{
		Use:   "rdapp",
		Short: "rdapp - Redshift Data API Postgres Proxy",
		Long:  `Use your favourite postgres tools to query redshift via redshift data api`,
		RunE: func(cmd *cobra.Command, args []string) error {
			logger := constructLogger()
			defer logger.Sync()
			cfg, err := config.LoadDefaultConfig(context.Background())
			if err != nil {
				logger.Fatal("error while loading aws config", zap.Error(err))
			}
			redshiftDataApiClient := redshiftdata.NewFromConfig(cfg)
			redshiftDataApiConfig := &rdapp.RedshiftDataAPIConfig{
				Database:          nil,
				ClusterIdentifier: getFlagValue(clusterIdentifier),
				DbUser:            getFlagValue(dbUser),
				SecretArn:         nil,
				WorkgroupName:     nil,
			}
			redshiftDataApiQueryHandler := rdapp.NewRedshiftDataApiQueryHandler(redshiftDataApiClient, redshiftDataApiConfig, logger)
			err = rdapp.NewPostgresRedshiftDataAPIProxy(listenAddress, redshiftDataApiQueryHandler.QueryHandler, logger).Run()
			if err != nil {
				logger.Error("error while creating postgres redshift proxy", zap.Error(err))
			}
			return nil
		},
	}
	rootCmd.Flags().StringVar(&listenAddress, "listen", "127.0.0.1:25432", "Listen address")
	rootCmd.Flags().StringVar(&dbUser, "db-user", "", "DB user")
	rootCmd.Flags().StringVar(&clusterIdentifier, "cluster-identifier", "", "DB user")
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func getFlagValue(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func constructLogger() *zap.Logger {
	productionConfig := zap.NewProductionConfig()
	productionConfig.EncoderConfig.TimeKey = "timestamp"
	productionConfig.EncoderConfig.EncodeTime = zapcore.RFC3339NanoTimeEncoder
	productionConfig.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	logger, _ := productionConfig.Build()
	return logger
}
