package server

import (
	"fmt"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
)

type httpServer struct {
	nsqd        *NSQD
	tlsEnabled  bool //TLS
	tlsRequired bool
	router      *gin.Engine //路由
}

func newHTTPServer(nsqd *NSQD, tlsEnable bool, tlsRequired bool) *httpServer {
	router := gin.Default()
	//对不被允许的方法返回405 Method Not Allowed
	router.HandleMethodNotAllowed = true
	//TODO :加入更多配置

	s := &httpServer{
		nsqd:        nsqd,
		tlsEnabled:  tlsEnable,
		tlsRequired: tlsRequired,
		router:      router,
	}
	//TODO: 注册方法

	return s
}

func (h *httpServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if !h.tlsEnabled && h.tlsRequired {
		resp := fmt.Sprintf(`{"message": "TLS_REQUIRED", "https_port": %d}`,
			h.nsqd.RealHTTPSAddr().Port)
		w.Header().Set("X-NSQ-Content-Type", "nsq; version=1.0")
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(403)
		io.WriteString(w, resp)
		return
	}

	h.router.ServeHTTP(w, req)
}
