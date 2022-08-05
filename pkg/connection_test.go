package rdapp_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v4"
	rdapp "github.com/kishaningithub/rdapp/pkg"
	"github.com/stretchr/testify/require"
)

func TestConnectivity(t *testing.T) {
	options := rdapp.Options{
		ListenAddress: "localhost:15432",
	}

	go func() {
		err := rdapp.RunPostgresRedshiftProxy(options)
		require.NoError(t, err)
	}()
	databaseUrl := "postgres://postgres:mypassword@localhost:15432/postgres"
	conn, err := pgx.Connect(context.Background(), databaseUrl)

	require.NoError(t, err)
	defer conn.Close(context.Background())
	connInfo := conn.Config()
	require.Equal(t, "localhost", connInfo.Host)
}

func TestQueryExecution(t *testing.T) {
	options := rdapp.Options{
		ListenAddress: "localhost:15432",
	}

	go func() {
		err := rdapp.RunPostgresRedshiftProxy(options)
		require.NoError(t, err)
	}()

	databaseUrl := "postgres://postgres:mypassword@localhost:15432/postgres"
	conn, err := pgx.Connect(context.Background(), databaseUrl)
	require.NoError(t, err)
	defer conn.Close(context.Background())
	connInfo := conn.Config()
	require.Equal(t, "localhost", connInfo.Host)
	var name string
	var weight int64
	err = conn.QueryRow(context.Background(), "select name, weight from widgets where id=$1", 42).Scan(&name, &weight)
	require.NoError(t, err)
	fmt.Println(name, weight)
}
