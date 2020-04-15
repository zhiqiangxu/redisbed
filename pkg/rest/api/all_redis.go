package api

import (
	"github.com/gin-gonic/gin"
	"github.com/zhiqiangxu/redisbed/pkg/service"
)

// AllRedis lists all redis service
func AllRedis(c *gin.Context) {
	output := service.Instance().AllRedis()
	sendoutput(c, output.Code, output)
}
