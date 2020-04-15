package gin

import (
	gin2 "github.com/gin-gonic/gin"
	"github.com/zhiqiangxu/redisbed/pkg/metrics"
	"github.com/zhiqiangxu/redisbed/pkg/rest/middleware"
)

// New returns a gin engine
func New() *gin2.Engine {
	g := gin2.New()

	mw := middleware.MetricLogger(metrics.RequestLatencyMetric, metrics.RequestCountMetric)
	g.Use(mw, gin2.Recovery())

	return g
}
