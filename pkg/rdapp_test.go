package rdapp_test

import (
	"context"
	"github.com/jackc/pgx/v4"
	"github.com/kishaningithub/rdapp/pkg"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"testing"
)

func TestRdapp(t *testing.T) {
	options := rdapp.Options{
		ListenAddress: "localhost:25432",
	}
	logger := constructLogger(t)
	go func() {
		err := rdapp.RunPostgresRedshiftProxy(options, logger)
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

	//t.Run("test query execution", func(t *testing.T) {
	//	var name string
	//	var weight int64
	//	err = conn.QueryRow(context.Background(), "select name, weight from widgets limit 1").Scan(&name, &weight)
	//	require.NoError(t, err)
	//	fmt.Println(name, weight)
	//})
}

func constructLogger(t *testing.T) *zap.Logger {
	t.Helper()
	productionConfig := zap.NewProductionConfig()
	productionConfig.EncoderConfig.TimeKey = "timestamp"
	productionConfig.EncoderConfig.EncodeTime = zapcore.RFC3339NanoTimeEncoder
	logger, _ := productionConfig.Build()
	return logger
}
