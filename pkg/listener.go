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
	log.Printf("started listening port=%s", options.ListenAddress)

	for {
		conn, err := listener.Accept()
		if err != nil {
			return fmt.Errorf("error while accepting a new connection: %w", err)
		}
		log.Printf("Accepted connection address=%s", conn.RemoteAddr())

		backend := NewRedshiftBackend(conn)
		go func() {
			err := backend.Run()
			if err != nil {
				log.Printf("error occurred error=%v", err)
			}
			log.Printf("Closed connection address=%s", conn.RemoteAddr())
		}()
	}
}
