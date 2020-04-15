package rest

import (
	"github.com/gin-gonic/gin"
	"github.com/zhiqiangxu/redisbed/pkg/rest/api"
	g "github.com/zhiqiangxu/redisbed/pkg/rest/gin"
)

// NewApp for public app
func NewApp() *gin.Engine {
	r := g.New()

	r.POST("/redis", api.NewRedis)
	r.GET("/redis", api.AllRedis)

	return r
}
