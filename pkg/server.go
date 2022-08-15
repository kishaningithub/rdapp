package rdapp

import (
	"fmt"
	"github.com/jackc/pgproto3/v2"
	"go.uber.org/zap"
	"io"
)

type RedshiftBackend struct {
	backend *pgproto3.Backend
	conn    io.ReadWriteCloser
	logger  *zap.Logger
}

func NewRedshiftBackend(conn io.ReadWriteCloser, logger *zap.Logger) *RedshiftBackend {
	backend := pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn)

	connHandler := &RedshiftBackend{
		backend: backend,
		conn:    conn,
		logger:  logger,
	}

	return connHandler
}

func (redshiftBackend *RedshiftBackend) Run() error {
	defer redshiftBackend.Close()

	err := redshiftBackend.handleStartup()
	if err != nil {
		return err
	}

	for {
		msg, err := redshiftBackend.backend.Receive()
		if err != nil {
			return fmt.Errorf("error receiving message: %w", err)
		}
		redshiftBackend.logger.Info("message received", zap.Any("message", msg))

		switch msg.(type) {
		case *pgproto3.Query, *pgproto3.Parse:
			response := "it works :-P"
			buf := (&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
				{
					Name:                 []byte("haha"),
					TableOID:             0,
					TableAttributeNumber: 0,
					DataTypeOID:          25,
					DataTypeSize:         -1,
					TypeModifier:         -1,
					Format:               0,
				},
			}}).Encode(nil)
			buf = (&pgproto3.DataRow{Values: [][]byte{[]byte(response)}}).Encode(buf)
			buf = (&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")}).Encode(buf)
			buf = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
			_, err = redshiftBackend.conn.Write(buf)
			if err != nil {
				return fmt.Errorf("error writing query response: %w", err)
			}
		case *pgproto3.Terminate:
			return nil
		default:
			return fmt.Errorf("received message other than Query from client: %#v", msg)
		}
	}
}

func (redshiftBackend *RedshiftBackend) handleStartup() error {
	startupMessage, err := redshiftBackend.backend.ReceiveStartupMessage()
	if err != nil {
		return fmt.Errorf("error receiving startup message: %w", err)
	}
	redshiftBackend.logger.Info("startup message received", zap.Any("message", startupMessage))

	switch startupMessage.(type) {
	case *pgproto3.StartupMessage:
		buf := (&pgproto3.AuthenticationOk{}).Encode(nil)
		// buf = (&pgproto3.ParameterStatus{Name: "application_name", Value: "psql"}).Encode(buf)
		buf = (&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"}).Encode(buf)
		buf = (&pgproto3.ParameterStatus{Name: "DateStyle", Value: "ISO"}).Encode(buf)
		buf = (&pgproto3.ParameterStatus{Name: "integer_datetimes", Value: "on"}).Encode(buf)
		buf = (&pgproto3.ParameterStatus{Name: "IntervalStyle", Value: "postgres"}).Encode(buf)
		buf = (&pgproto3.ParameterStatus{Name: "is_superuser", Value: "on"}).Encode(buf)
		buf = (&pgproto3.ParameterStatus{Name: "server_encoding", Value: "UTF8"}).Encode(buf)
		buf = (&pgproto3.ParameterStatus{Name: "server_version", Value: "11.5"}).Encode(buf)
		buf = (&pgproto3.ParameterStatus{Name: "session_authorization", Value: "jack"}).Encode(buf)
		buf = (&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"}).Encode(buf)
		buf = (&pgproto3.ParameterStatus{Name: "TimeZone", Value: "US/Central"}).Encode(buf)
		buf = (&pgproto3.BackendKeyData{ProcessID: 31007, SecretKey: 1013083042}).Encode(buf)
		buf = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
		_, err = redshiftBackend.conn.Write(buf)
		if err != nil {
			return fmt.Errorf("error sending ready for query: %w", err)
		}
	case *pgproto3.SSLRequest:
		_, err = redshiftBackend.conn.Write([]byte("N"))
		if err != nil {
			return fmt.Errorf("error sending deny SSL request: %w", err)
		}
		return redshiftBackend.handleStartup()
	default:
		return fmt.Errorf("unknown startup message: %#v", startupMessage)
	}

	return nil
}

func (redshiftBackend *RedshiftBackend) Close() error {
	return redshiftBackend.conn.Close()
}
