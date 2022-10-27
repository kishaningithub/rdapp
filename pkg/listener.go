package rdapp

import (
	"crypto/tls"
	"fmt"
	wire "github.com/jeroenrinzema/psql-wire"
	"go.uber.org/zap"
)

type PostgresRedshiftProxy interface {
	Run() error
}

type postgresRedshiftProxy struct {
	listenAddress string
	simpleQueryFn wire.SimpleQueryFn
	logger        *zap.Logger
}

func NewPostgresRedshiftDataAPIProxy(listenAddress string, simpleQueryFn wire.SimpleQueryFn, logger *zap.Logger) PostgresRedshiftProxy {
	return &postgresRedshiftProxy{
		listenAddress: listenAddress,
		simpleQueryFn: simpleQueryFn,
		logger:        logger,
	}
}

func (proxy *postgresRedshiftProxy) Run() error {
	server, err := wire.NewServer(wire.Logger(proxy.logger), wire.SimpleQuery(proxy.simpleQueryFn), wire.ClientAuth(tls.NoClientCert))
	if err != nil {
		return fmt.Errorf("error while instantiating server: %w", err)
	}
	err = server.ListenAndServe(proxy.listenAddress)
	if err != nil {
		return fmt.Errorf("error while listening to %s: %w", proxy.listenAddress, err)
	}
	return nil
}
