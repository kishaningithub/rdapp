package componenttest

import (
	"context"
	"database/sql"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	_ "github.com/jackc/pgx/v5/stdlib"
	rdapp "github.com/kishaningithub/rdapp/pkg"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"testing"
	"time"
)

func TestRedshiftDataApiPostgresProxy(t *testing.T) {
	listenAddress := ":35432"
	logger, err := zap.NewDevelopment()

	require.NoError(t, err)
	cfg, err := config.LoadDefaultConfig(context.Background())
	require.NoError(t, err)
	redshiftDataAPIConfig := rdapp.RedshiftDataAPIConfig{
		Database:      aws.String("dev"),
		WorkgroupName: aws.String("rdapp"),
	}
	proxy := rdapp.ConstructProxy(cfg, redshiftDataAPIConfig, logger, listenAddress)
	go func() {
		logger.Info("Starting test instance of postgres redshift proxy...")
		err := proxy.Run()
		require.NoError(t, err)
	}()

	databaseUrl := "postgres://postgres:mypassword@localhost:35432/postgres"
	conn, err := sql.Open("pgx", databaseUrl)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = conn.Close()
	})

	t.Run("test connectivity", func(t *testing.T) {
		err := conn.Ping()
		require.NoError(t, err)
	})

	t.Run("simple query execution", func(t *testing.T) {
		var intValue int
		var stringValue string
		var boolValue bool
		var timeStamp time.Time
		rows, err := conn.Query("select 1, 'name', true, now()")
		require.NoError(t, err)
		t.Cleanup(func() {
			rows.Close()
		})
		require.True(t, rows.Next())
		err = rows.Scan(&intValue, &stringValue, &boolValue, &timeStamp)
		require.NoError(t, err)
		require.Equal(t, 1, intValue)
		require.Equal(t, "name", stringValue)
		require.Equal(t, true, boolValue)
		require.WithinDuration(t, time.Now().UTC(), timeStamp, 10*time.Second)
	})

	t.Run("prepared statement query execution using dollar parm style", func(t *testing.T) {
		query := `
      select * 
      from (
		  select 1 id
		  union all
		  select 2 id
		  union all
		  select 3 id
      )
      where id > $1
   `
		stmt, err := conn.Prepare(query)
		require.NoError(t, err)
		t.Cleanup(func() {
			stmt.Close()
		})
		rows, err := stmt.Query(1)
		require.NoError(t, err)
		t.Cleanup(func() {
			rows.Close()
		})
		var ids []int
		for rows.Next() {
			var id int
			err := rows.Scan(&id)
			require.NoError(t, err)
			ids = append(ids, id)
		}
		require.NoError(t, rows.Err())
		require.Equal(t, []int{2, 3}, ids)
	})

	t.Run("prepared statement query execution using question mark style", func(t *testing.T) {
		query := `
      select * 
      from (
		  select 1 id
		  union all
		  select 2 id
		  union all
		  select 3 id
      )
      where id > ?
   `
		stmt, err := conn.Prepare(query)
		require.NoError(t, err)
		t.Cleanup(func() {
			stmt.Close()
		})
		rows, err := stmt.Query(1)
		require.NoError(t, err)
		t.Cleanup(func() {
			rows.Close()
		})
		var ids []int
		for rows.Next() {
			var id int
			err := rows.Scan(&id)
			require.NoError(t, err)
			ids = append(ids, id)
		}
		require.NoError(t, rows.Err())
		require.Equal(t, []int{2, 3}, ids)
	})
}
