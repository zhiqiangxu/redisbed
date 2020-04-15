package model

import "time"

// RedisInfo for redis info
type RedisInfo struct {
	Port    uint16
	Created time.Time
}
