package rdapp

import (
	"fmt"
	"log"
	"net"
)

type Options struct {
	ListenAddress string
}

func RunPostgresRedshiftProxy(options Options) error {
	listener, err := net.Listen("tcp", options.ListenAddress)
	if err != nil {
		return fmt.Errorf("error while listening to %s: %w", options.ListenAddress, err)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			return fmt.Errorf("error while accepting a new connection: %w", err)
		}
		log.Println("Accepted connection from", conn.RemoteAddr())

		backend := NewRedshiftBackend(conn)
		go func() {
			err := backend.Run()
			if err != nil {
				log.Println(err)
			}
			log.Println("Closed connection from", conn.RemoteAddr())
		}()
	}
}
