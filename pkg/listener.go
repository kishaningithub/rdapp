package rdapp

import (
	"context"
	"crypto/tls"
	"fmt"
	wire "github.com/jeroenrinzema/psql-wire"
	"go.uber.org/zap"
)

type Options struct {
	ListenAddress string
}

type PostgresRedshiftProxy interface {
	Run() error
}

type postgresRedshiftProxy struct {
	options Options
	logger  *zap.Logger
}

func NewPostgresRedshiftDataAPIProxy(options Options, logger *zap.Logger) PostgresRedshiftProxy {
	return &postgresRedshiftProxy{
		options: options,
		logger:  logger,
	}
}

func (proxy *postgresRedshiftProxy) Run() error {
	server, err := wire.NewServer(wire.Logger(proxy.logger), wire.SimpleQuery(proxy.queryHandler), wire.ClientAuth(tls.NoClientCert))
	if err != nil {
		return fmt.Errorf("error while instantiating server: %w", err)
	}
	err = server.ListenAndServe(proxy.options.ListenAddress)
	if err != nil {
		return fmt.Errorf("error while listening to %s: %w", proxy.options.ListenAddress, err)
	}
	return nil
}

func (proxy *postgresRedshiftProxy) queryHandler(ctx context.Context, query string, writer wire.DataWriter) error {
	proxy.logger.Info("received query", zap.String("query", query))
	return writer.Complete("OK")
}
