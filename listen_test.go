package sdk

import (
	"net"
	"os"
	"path/filepath"
	"testing"
)

func TestInitListenerStdioDefault(t *testing.T) {
	t.Setenv(ListenEnv, "")
	os.Unsetenv(ListenEnv)

	// The closer is deliberately not closed: in stdio mode it wraps the
	// process's real stdin/stdout, and closing those would silence the
	// remaining tests.
	listener, _, err := initListener()
	if err != nil {
		t.Fatalf("initListener: %v", err)
	}

	if _, ok := listener.(*singleConnListener); !ok {
		t.Fatalf("expected the stdio single-conn listener, got %T", listener)
	}
}

func TestInitListenerUnixSocket(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "plugin.sock")
	t.Setenv(ListenEnv, "unix://"+sock)

	listener, closer, err := initListener()
	if err != nil {
		t.Fatalf("initListener: %v", err)
	}
	defer closer.Close()

	fi, err := os.Stat(sock)
	if err != nil {
		t.Fatalf("stat socket: %v", err)
	}
	if perm := fi.Mode().Perm(); perm != 0666 {
		t.Fatalf("socket mode = %o, want 0666", perm)
	}

	// Prove the listener actually accepts connections.
	done := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			conn.Close()
		}
		done <- err
	}()
	conn, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	conn.Close()
	if err := <-done; err != nil {
		t.Fatalf("accept: %v", err)
	}
}

func TestInitListenerRejectsNonUnix(t *testing.T) {
	t.Setenv(ListenEnv, "tcp://0.0.0.0:9876")

	if _, _, err := initListener(); err == nil {
		t.Fatal("expected initListener to reject a non-unix address")
	}
}
