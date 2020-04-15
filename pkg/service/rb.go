package service

import "sync"

// RedisBed service
type RedisBed struct {
}

var (
	instanceRedisBed *RedisBed
	lockRedisBed     sync.Mutex
)

// Instance is singleton for RedisBed
func Instance() *RedisBed {
	if instanceRedisBed != nil {
		return instanceRedisBed
	}

	lockRedisBed.Lock()
	defer lockRedisBed.Unlock()
	if instanceRedisBed != nil {
		return instanceRedisBed
	}

	instanceRedisBed = &RedisBed{}

	return instanceRedisBed
}
