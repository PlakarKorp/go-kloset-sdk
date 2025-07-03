package sdk

import (
	"os"
	"fmt"
	"net"
)

// singleConnListener is a net.Listener that accepts only one connection.
type singleConnListener struct {
	conn net.Conn
	used bool
}

// Accept returns the connection once, then blocks forever.
func (l *singleConnListener) Accept() (net.Conn, error) {
	if l.used {
		<-make(chan struct{}) // Block forever.
	}
	l.used = true
	return l.conn, nil
}

// Close closes the underlying connection.
func (l *singleConnListener) Close() error {
	return l.conn.Close()
}

// Addr returns the local address of the listener.
func (l *singleConnListener) Addr() net.Addr {
	return l.conn.LocalAddr()
}

// InitConn initializes the gRPC socket connection from stdin.
// It returns the net.Conn and a Listener wrapping it.
func InitConn() (net.Conn, net.Listener, error) {
	conn, err := net.FileConn(os.Stdin)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to convert stdin to net.Conn: %w", err)
	}

	listener := &singleConnListener{conn: conn}
	return conn, listener, nil
}
