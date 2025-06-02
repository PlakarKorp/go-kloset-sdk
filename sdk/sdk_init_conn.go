package sdk

import (
	"os"
	"fmt"
	"net"
)

type singleConnListener struct {
	conn net.Conn
	used bool
}

func (l *singleConnListener) Accept() (net.Conn, error) {
	if l.used {
		<-make(chan struct{})
	}
	l.used = true
	return l.conn, nil
}

func (l *singleConnListener) Close() error {
	return l.conn.Close()
}

func (l *singleConnListener) Addr() net.Addr {
	return l.conn.LocalAddr()
}

func InitConn() (net.Conn, net.Listener, error) {
	conn, err := net.FileConn(os.Stdin)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to convert stdin to net.Conn: %w", err)
	}

	listener := &singleConnListener{conn: conn}
	return conn, listener, nil
}