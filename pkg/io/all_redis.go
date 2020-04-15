package io

import "github.com/zhiqiangxu/redisbed/pkg/model"

// AllRedisOutput for output
type AllRedisOutput struct {
	BaseResp
	Info []model.RedisInfo
}
