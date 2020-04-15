package service

import (
	"github.com/zhiqiangxu/redisbed/pkg/model"
	"github.com/zhiqiangxu/redisbed/pkg/redis"
)

// StartAllRedis impl
func (rb *RedisBed) StartAllRedis() (err error) {
	infos, err := model.RedisInfoManager().GetAll()
	if err != nil {
		return
	}

	for _, info := range infos {
		redis.StartByPort(info.Port)
	}
	return
}
