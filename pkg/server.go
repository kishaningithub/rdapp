package rdapp

import (
	"fmt"
	"io"
	"log"

	"github.com/jackc/pgproto3/v2"
)

type RedshiftBackend struct {
	backend *pgproto3.Backend
	conn    io.ReadWriteCloser
}

func NewRedshiftBackend(conn io.ReadWriteCloser) *RedshiftBackend {
	backend := pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn)

	connHandler := &RedshiftBackend{
		backend: backend,
		conn:    conn,
	}

	return connHandler
}

func (p *RedshiftBackend) Run() error {
	defer p.Close()

	err := p.handleStartup()
	if err != nil {
		return err
	}

	for {
		msg, err := p.backend.Receive()
		if err != nil {
			return fmt.Errorf("error receiving message: %w", err)
		}
		log.Printf("received message %#v", msg)

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
			_, err = p.conn.Write(buf)
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

func (p *RedshiftBackend) handleStartup() error {
	startupMessage, err := p.backend.ReceiveStartupMessage()
	if err != nil {
		return fmt.Errorf("error receiving startup message: %w", err)
	}
	log.Printf("startup message received %#v", startupMessage)

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
		_, err = p.conn.Write(buf)
		if err != nil {
			return fmt.Errorf("error sending ready for query: %w", err)
		}
	case *pgproto3.SSLRequest:
		_, err = p.conn.Write([]byte("N"))
		if err != nil {
			return fmt.Errorf("error sending deny SSL request: %w", err)
		}
		return p.handleStartup()
	default:
		return fmt.Errorf("unknown startup message: %#v", startupMessage)
	}

	return nil
}

func (p *RedshiftBackend) Close() error {
	return p.conn.Close()
}
