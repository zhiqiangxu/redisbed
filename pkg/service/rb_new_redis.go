package service

import (
	"fmt"
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

	var (
		l   net.Listener
		err error
	)
	if input.Port == 0 {
		l, err = port.ListenTCP()
	} else {
		l, err = net.Listen("tcp", fmt.Sprintf(":%d", input.Port))
	}

	if err != nil {
		output.Code = http.StatusInternalServerError
		output.Msg = err.Error()
		return
	}

	output.Port = uint16(l.Addr().(*net.TCPAddr).Port)

	go redis.Start(l)

	info := model.RedisInfo{Port: uint16(l.Addr().(*net.TCPAddr).Port), Created: time.Now()}
	err = model.RedisInfoManager().Insert(info)
	if err != nil {
		output.Code = http.StatusInternalServerError
		output.Msg = err.Error()
		return
	}

	return
}
