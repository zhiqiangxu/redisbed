package rest

import (
	"context"
	"net/http"
	"os"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/zhiqiangxu/redisbed/pkg/config"
	"github.com/zhiqiangxu/redisbed/pkg/logger"
	"github.com/zhiqiangxu/redisbed/pkg/store"
	"github.com/zhiqiangxu/util/signal"
	"go.uber.org/zap"
)

// Server for redisbed
type Server struct {
	app *gin.Engine
}

// NewServer creates a Server
func NewServer() *Server {

	s := &Server{}

	s.app = NewApp()

	return s
}

// Start the server
func (s *Server) Start() {
	conf := config.Load()

	// bootstap
	err := store.Instance().Bootstrap()
	if err != nil {
		logger.Instance().Fatal("Bootstrap", zap.Error(err))
	}

	server := &http.Server{Addr: conf.HTTPAddr, Handler: s.app}

	signal.SetupHandler(func(s os.Signal) {
		// exit on signal

		err = store.Instance().Finalize()
		logger.Instance().Error("Finalize", zap.Error(err))

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
		defer cancel()
		err := server.Shutdown(ctx)
		logger.Instance().Error("Shutdown", zap.Error(err))
		os.Exit(1)
	}, os.Interrupt, os.Kill, syscall.SIGTERM, syscall.SIGUSR1, syscall.SIGQUIT)

	err = server.ListenAndServe()
	logger.Instance().Error("ListenAndServe", zap.Error(err))

}
