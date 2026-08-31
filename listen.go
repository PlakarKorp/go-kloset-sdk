package sdk

import (
	"fmt"
	"io"
	"net"
	"os"
	"strings"
)

// When running over a container plakar sets this to the path of the socket we
// should be listening on. Empty when running on a default runner.
const ListenEnv = "PLAKAR_PLUGIN_LISTEN"

func initListener() (net.Listener, io.Closer, error) {
	addr := os.Getenv(ListenEnv)
	if addr == "" {
		conn, listener, err := InitConn()
		if err != nil {
			return nil, nil, err
		}
		return listener, conn, nil
	}

	path, ok := strings.CutPrefix(addr, "unix://")
	if !ok || len(path) == 0 {
		// TODO: If we want to support more than linux we need to swap this to a
		// tcp socket, but it needs a bit more coordination from plakar.
		return nil, nil, fmt.Errorf("unsupported %s address %q: only unix:// is supported", ListenEnv, addr)
	}

	listener, err := net.Listen("unix", path)
	if err != nil {
		return nil, nil, err
	}

	// The socket is created with the container-side uid; open it up and let
	// the 0700 host-side parent dir gate access instead.
	if err := os.Chmod(path, 0666); err != nil {
		listener.Close()
		return nil, nil, err
	}
	return listener, listener, nil
}
