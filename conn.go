package sdk

import (
	"io"
	"net"
	"sync"
)

// singleConnListener is a net.Listener that accepts only one connection.
type singleConnListener struct {
	notify <-chan struct{}
	conn   net.Conn
	used   bool
}

// Accept returns the connection once, then blocks forever.
func (l *singleConnListener) Accept() (net.Conn, error) {
	if l.used {
		<-l.notify
		return nil, io.ErrClosedPipe
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

// InitConn initializes the gRPC socket connection from stdio.
// It returns the net.Conn and a Listener wrapping it.
func InitConn() (net.Conn, net.Listener, error) {
	conn := NewStdioConn()

	ch := make(chan struct{})
	var mtx sync.Mutex
	conn.onclose = func() {
		mtx.Lock()
		if ch != nil {
			close(ch)
			ch = nil
		}
		mtx.Unlock()
	}

	listener := &singleConnListener{notify: ch, conn: conn}
	return conn, listener, nil
}
