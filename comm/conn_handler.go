// Package comm contains functions and types from communication between music-sync clients and the server
package comm

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"net"
	"os"
)

type packageHandler interface {
	Handle(message proto.Message, sender net.Conn)
}

func handleConnection(conn net.Conn, h packageHandler) {
	defer conn.Close()
	handlePackages(conn, h)
}

// handlePackages reads and handles messages from conn until reading fails. In contrast to
// handleConnection, it does not close conn; the caller is responsible for closing it.
func handlePackages(conn net.Conn, h packageHandler) {
	for {
		m, err := readWire(conn)
		if err != nil {
			logger.Infof("failed to read data from %s: %v. closing connection", conn.RemoteAddr(), err)
			return
		}
		h.Handle(m, conn)
	}
}

// NewClientHandler is called when a new client connects to the server (with channel -1)
// and when a client subscribes to a channel.
var NewClientHandler func(channel Channel, conn MessageSender)

// StartServer starts a music-sync server listening at address and returns a MessageSender to broadcast
// to clients
func StartServer(address string) (MessageSender, error) {
	logger.Infof("starting server at %s", address)

	l, err := net.Listen("tcp", address)
	if err != nil {
		logger.Fatalf("failed to start server at %s: %v", address, err)
		return nil, fmt.Errorf("failed to start server at %s: %v", address, err)
	}
	logger.Infof("server running at %s", address)

	mms := &multiMessageSender{connections: make([]net.Conn, 0), channels: make(map[net.Conn][]Channel)}
	go serverConnectionAcceptor(mms, l)

	return mms, nil
}

func serverConnectionAcceptor(mms *multiMessageSender, l net.Listener) {
	h := newServerPackageHandler(mms)
	for {
		conn, err := l.Accept()
		if err != nil {
			logger.Fatalf("failed to accept connection: %v", err)
			break
		}
		go func(conn net.Conn) {
			mms.AddConn(conn)
			if NewClientHandler != nil {
				NewClientHandler(-1, mms.senderFor(conn))
			}
			// Do not close the connection here. DelConn marks the connection as closed before it
			// closes it, so a broadcast that is in progress cannot write to it after that point.
			handlePackages(conn, h)
			mms.DelConn(conn)
		}(conn)
	}
}

// ConnectToServer connects to the server at server and returns a MessageSender to communicate with the master
func ConnectToServer(master string, handler TypedPackageHandler) (MessageSender, error) {
	logger.Infof("connecting to master at %s", master)
	conn, err := net.Dial("tcp", master)
	if err != nil {
		logger.Fatalf("could not connect to master at %s: %v", master, err)
		return nil, fmt.Errorf("could not connect to master at %s: %v", master, err)
	}
	logger.Infof("connected to master at %s", master)
	go func() {
		handleConnection(conn, handler)
		logger.Fatalf("connection to master closed")
		os.Exit(1)
	}()
	return &singleMessageSender{connection: conn}, nil
}
