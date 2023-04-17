package abstract

import (
	"MQ/mlog"
	"fmt"
	"net"
	"net/http"
	"strings"
)

func Serve(listener net.Listener, handler http.Handler, proto string) error {
	mlog.Info("%s: listening on %s", proto, listener.Addr())

	server := &http.Server{
		Handler: handler,
	}
	err := server.Serve(listener)
	// theres no direct way to detect this error because it is not exposed
	if err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
		return fmt.Errorf("http.Serve() error - %s", err)
	}

	mlog.Info("%s: closing %s", proto, listener.Addr())

	return nil
}
