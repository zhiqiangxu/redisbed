package service

import (
	"net"
	"net/http"
	"time"

	"github.com/zhiqiangxu/redisbed/pkg/io"
	"github.com/zhiqiangxu/redisbed/pkg/model"
	"github.com/zhiqiangxu/redisbed/pkg/redis"
	"github.com/zhiqiangxu/util/port"
)

// NewRedis impl
func (rb *RedisBed) NewRedis(input io.NewRedisInput) (output io.NewRedisOutput) {

	l, err := port.ListenTCP()
	if err != nil {
		output.Code = http.StatusInternalServerError
		output.Msg = err.Error()
		return
	}

	err = redis.Start(l)
	if err != nil {
		output.Code = http.StatusInternalServerError
		output.Msg = err.Error()
		return
	}

	info := model.RedisInfo{Port: uint16(l.Addr().(*net.TCPAddr).Port), Created: time.Now()}
	err = model.RedisInfoManager().Insert(info)
	if err != nil {
		output.Code = http.StatusInternalServerError
		output.Msg = err.Error()
		return
	}

	return
}
