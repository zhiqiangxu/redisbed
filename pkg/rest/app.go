package rest

import (
	"github.com/gin-gonic/gin"
	g "github.com/zhiqiangxu/redisbed/pkg/rest/gin"
)

// NewApp for public app
func NewApp() *gin.Engine {
	r := g.New()

	return r
}
