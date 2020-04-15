package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/zhiqiangxu/redisbed/pkg/io"
	"github.com/zhiqiangxu/redisbed/pkg/service"
)

// NewRedis for create redis service
func NewRedis(c *gin.Context) {
	var input io.NewRedisInput
	if err := c.ShouldBind(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": err.Error()})
		return
	}

	output := service.Instance().NewRedis(input)
	sendoutput(c, output.Code, output)
}

func sendoutput(c *gin.Context, code int, output interface{}) {
	if code == 0 {
		code = http.StatusOK
	}

	c.JSON(code, output)
}
