package rdapp_test

import (
	"context"
	"github.com/jackc/pgx/v4"
	rdapp "github.com/kishaningithub/rdapp/pkg"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRdapp(t *testing.T) {
	options := rdapp.Options{
		ListenAddress: "localhost:25432",
	}
	go func() {
		err := rdapp.RunPostgresRedshiftProxy(options)
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
	//	err = conn.QueryRow(context.Background(), "select name, weight from widgets where id=$1", 42).Scan(&name, &weight)
	//	require.NoError(t, err)
	//	fmt.Println(name, weight)
	//})
}
