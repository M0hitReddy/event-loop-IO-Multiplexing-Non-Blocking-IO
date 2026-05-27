// Command main implements a minimal TCP server that echoes received messages back to clients.
// It serves as a simplified clone of Redis's basic client-server interaction model,
// listening on localhost:8080 and handling one connection at a time.
package main

import (
	"io"
	"log"
	"net"
)

func main() {
	addr := "127.0.0.1:8080"
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", addr, err)
	}
	log.Printf("Listening on %s", addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}

		log.Printf("Accepted connection from %s", conn.RemoteAddr())
		handleConnection(conn)
	}
}

// handleConnection reads messages from a client connection and echoes them back.
// It returns when the client disconnects or an error occurs.
func handleConnection(conn net.Conn) {
	defer conn.Close()

	buf := make([]byte, 1024)

	for {
		n, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				log.Printf("Client %s closed the connection", conn.RemoteAddr())
			} else {
				log.Printf("Read error from %s: %v", conn.RemoteAddr(), err)
			}
			return
		}

		message := string(buf[:n])
		log.Printf("%s > %s", conn.RemoteAddr(), message)

		if err := echo(conn, message); err != nil {
			log.Printf("Write error to %s: %v", conn.RemoteAddr(), err)
			return
		}
	}
}

// echo sends the given message back to the client connection.
func echo(conn net.Conn, message string) error {
	_, err := conn.Write([]byte(message))
	return err
}
