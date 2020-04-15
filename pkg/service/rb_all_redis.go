package service

import (
	"net/http"

	"github.com/zhiqiangxu/redisbed/pkg/io"
	"github.com/zhiqiangxu/redisbed/pkg/model"
)

// AllRedis impl
func (rb *RedisBed) AllRedis() (output io.AllRedisOutput) {

	info, err := model.RedisInfoManager().GetAll()
	if err != nil {
		output.Code = http.StatusInternalServerError
		output.Msg = err.Error()
		return
	}

	output.Info = info
	return
}
