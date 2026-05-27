//go:build linux

package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"syscall"
)

type core struct{ fd int }

func (f *core) Read(b []byte) (int, error)  { return syscall.Read(f.fd, b) }
func (f *core) Write(b []byte) (int, error) { return syscall.Write(f.fd, b) }

// concurrent connections count
var concConn = 0

func main() {
	//addr := "127.0.0.1:8080"
	// create a socket to listen on
	// SOCK_CLOEXEC sets FD_CLOEXEC on socket fd
	// When the child calls exec(), the kernel automatically closes any fd with this flag set — before the new program even starts.
	serverFD, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM|syscall.SOCK_CLOEXEC, 0) // SOCK_CLOEXEC closes the fd when exec() is run in child process after fork()
	if err != nil {
		log.Fatalf("Failed to create socket: %v", err)
	}
	defer func(fd int) {
		err := syscall.Close(fd)
		if err != nil {
			log.Printf("Failed to close socket: %v", err)
		}
	}(serverFD)

	// this helps to reuse the same port if its in TIME_WAIT state. Or else we will get port already in use
	if err := syscall.SetsockoptInt(serverFD, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err != nil {
		panic(err)
	}

	// create socket addr to listen on all interfaces on port {port}
	addr := syscall.SockaddrInet4{
		Port: port,
		Addr: [4]byte{0, 0, 0, 0}, // Listen on all interfaces
	}

	// bind the server socket to the port/addr
	err = syscall.Bind(serverFD, &addr)
	if err != nil {
		log.Fatalf("Failed to bind socket: %v", err)
	}

	// start listening for connections on server socket
	err = syscall.Listen(serverFD, backlog)
	if err != nil {
		log.Fatalf("Failed to listen on socket: %v", err)
	}
	log.Print("Listening on ", port)

	// create epoll object
	// EPOLL_CLOEXEC sets FD_CLOEXEC on epoll fd
	// This automatically closes the fd's passed to child process by fork(), when exec() runs
	epollFD, _ := syscall.EpollCreate1(syscall.EPOLL_CLOEXEC)
	defer syscall.Close(epollFD)

	// Register the server socket/FD to the epoll object with EPOLLIN event
	// EPOLLIN is triggered only when there's incoming data/connections on that FD
	err = syscall.EpollCtl(epollFD, syscall.EPOLL_CTL_ADD, serverFD, &syscall.EpollEvent{Events: syscall.EPOLLIN, Fd: int32(serverFD)})
	if err != nil {
		log.Fatalf("Failed to add server socket to epoll: %v", err)
	}

	// events buffer to store epoll events returned by epoll_wait syscall
	var events = make([]syscall.EpollEvent, epollBatch) // we will process epollBatch no. of events at a time

	for {
		// -1 is wait timeout. It gets blocked here untill epoll has some FD's ready
		// epoll_wait returns the FD's that are ready with data that we registered using epollcreate1 and length
		n, err := syscall.EpollWait(epollFD, events, -1)
		if err != nil {
			if errors.Is(err, syscall.EINTR) {
				continue // interrupted by signal, just retry
			}
			log.Fatalf("Epoll wait error: %v", err)
		}

		//log.Print("Length of epoll wait FDs: ", n)

		// handle the events
		for i := 0; i < n; i++ {
			// handle the connection events because the data is ready on socketFD (only listening for incoming conns)
			if events[i].Fd == int32(serverFD) {
				// accept the conn. This creates a socketFD for that client conn
				socketFD, socketAddr, err := syscall.Accept(serverFD)
				if err != nil {
					log.Printf("Accept error: %v", err)
					continue
				}
				concConn++
				var clientAddr string
				if sa, ok := socketAddr.(*syscall.SockaddrInet4); ok {
					clientAddr = fmt.Sprintf("%s:%d", net.IP(sa.Addr[:]), sa.Port)
				}
				log.Printf("Accepted connection from %s | CONCURRENT CONN: %d", clientAddr, concConn)

				// ** Add/register the new socket FD to the epoll object with EPOLLIN event.
				err = syscall.EpollCtl(
					epollFD,
					syscall.EPOLL_CTL_ADD,
					socketFD,
					&syscall.EpollEvent{Events: syscall.EPOLLIN, Fd: int32(socketFD)},
				)
				if err != nil {
					log.Printf("Failed to add client socket to epoll: %v", err)
					syscall.Close(socketFD)
					concConn--
					continue
				}
			} else { // if its not a connection event, then its some data coming from existing client connection/socket
				// handle client socket events
				socketFD := int(events[i].Fd)
				buff := make([]byte, 1024)
				//c := &core{fd: serverFD}
				n, err := (&core{fd: socketFD}).Read(buff)

				if err != nil {
					if errors.Is(err, io.EOF) {
						log.Printf("Client %d closed the connection", socketFD)
					} else {
						log.Printf("Read error from client %d: %v", socketFD, err)
					}
					// remove the FD from EPOLL when client disconneects and close socket/FD
					syscall.EpollCtl(epollFD, syscall.EPOLL_CTL_DEL, socketFD, nil)
					syscall.Close(socketFD)
					concConn--
					continue
				}

				message := bytes.TrimSpace(buff[:n])
				log.Printf("Received from client %d: %s", socketFD, message)
				if err = echo(&core{fd: socketFD}, string(buff[:n])); err != nil {
					log.Printf("Write error to client %d: %v", socketFD, err)
					syscall.EpollCtl(epollFD, syscall.EPOLL_CTL_DEL, socketFD, nil)
					syscall.Close(socketFD)
					concConn--
				}
			}
		}

	}

}

func echo(conn io.ReadWriter, message string) error {
	//value, _ := conn.(*core)
	//print(value.)
	_, err := conn.Write([]byte(message))
	return err
}

// handleConnection reads messages from a client connection and echoes them back.
// It returns when the client disconnects or an error occurs.
//func handleConnection(conn io.ReadWriter) {
//	//defer conn.Close()
//
//	buf := make([]byte, 1024)
//
//	for {
//		n, err := conn.Read(buf)
//		if err != nil {
//			if err == io.EOF {
//				log.Printf("Client %s closed the connection", conn.RemoteAddr())
//			} else {
//				log.Printf("Read error from %s: %v", conn.RemoteAddr(), err)
//			}
//			return
//		}
//
//		message := string(buf[:n])
//		log.Printf("%s > %s", conn.RemoteAddr(), message)
//
//		if err := echo(conn, message); err != nil {
//			log.Printf("Write error to %s: %v", conn.RemoteAddr(), err)
//			return
//		}
//	}
//}
