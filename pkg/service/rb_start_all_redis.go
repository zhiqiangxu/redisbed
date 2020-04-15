package service

import (
	"github.com/zhiqiangxu/redisbed/pkg/logger"
	"github.com/zhiqiangxu/redisbed/pkg/model"
	"github.com/zhiqiangxu/redisbed/pkg/redis"
	"go.uber.org/zap"
)

// StartAllRedis impl
func (rb *RedisBed) StartAllRedis() (err error) {
	infos, err := model.RedisInfoManager().GetAll()
	if err != nil {
		return
	}

	logger.Instance().Info("StartAllRedis", zap.Any("redis", infos))

	for _, info := range infos {
		err = redis.StartByPort(info.Port)
		if err != nil {
			return
		}
	}
	return
}
