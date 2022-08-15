package rdapp

import (
	"fmt"
	"go.uber.org/zap"
	"net"
)

type Options struct {
	ListenAddress string
}

func RunPostgresRedshiftProxy(options Options, logger *zap.Logger) error {
	listener, err := net.Listen("tcp", options.ListenAddress)
	if err != nil {
		return fmt.Errorf("error while listening to %s: %w", options.ListenAddress, err)
	}
	logger.Info("listening for incoming connections", zap.String("port", options.ListenAddress))

	for {
		conn, err := listener.Accept()
		if err != nil {
			return fmt.Errorf("error while accepting a new connection: %w", err)
		}
		logger.Info("accepted new connection", zap.String("address", conn.RemoteAddr().String()))

		backend := NewRedshiftBackend(conn, logger)
		go func() {
			err := backend.Run()
			if err != nil {
				logger.Error("error occurred", zap.Error(err))
			}
			logger.Info("closed  connection", zap.String("address", conn.RemoteAddr().String()))
		}()
	}
}
