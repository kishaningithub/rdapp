package rdapp_test

import (
	"context"
	"github.com/jackc/pgx/v4"
	wire "github.com/jeroenrinzema/psql-wire"
	rdapp "github.com/kishaningithub/rdapp/pkg"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"testing"
)

func TestRedshiftDataAPIProxy(t *testing.T) {
	listenAddress := "localhost:25432"
	logger := constructLogger(t)
	go func() {
		logger.Info("Starting test instance of postgres redshift proxy...")
		err := rdapp.NewPostgresRedshiftDataAPIProxy(listenAddress, func(ctx context.Context, query string, writer wire.DataWriter, parameters []string) error {
			return writer.Complete("OK")
		}, logger).Run()
		require.NoError(t, err)
	}()
	databaseUrl := "postgres://postgres:mypassword@localhost:25432/postgres"
	conn, err := pgx.Connect(context.Background(), databaseUrl)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, conn.Close(context.Background()))
	})

	t.Run("test connectivity", func(t *testing.T) {
		connInfo := conn.Config()
		require.Equal(t, "localhost", connInfo.Host)
	})

	t.Run("test query execution", func(t *testing.T) {
		rows, err := conn.Query(context.Background(), "select name, weight from widgets limit 1")
		require.NoError(t, err)
		require.NoError(t, rows.Err())
	})
}

func constructLogger(t *testing.T) *zap.Logger {
	t.Helper()
	productionConfig := zap.NewProductionConfig()
	productionConfig.EncoderConfig.TimeKey = "timestamp"
	productionConfig.EncoderConfig.EncodeTime = zapcore.RFC3339NanoTimeEncoder
	productionConfig.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	logger, _ := productionConfig.Build()
	return logger
}
