package model

import (
	"sync"

	"github.com/zhiqiangxu/redisbed/pkg/store"
)

// RedisInfoMgr for RedisInfo crud
type RedisInfoMgr struct {
}

var (
	redisInfoMgr     *RedisInfoMgr
	redisInfoMgrLock sync.Mutex
)

// RedisInfoManager is singleton for RedisInfoMgr
func RedisInfoManager() *RedisInfoMgr {
	if redisInfoMgr != nil {
		return redisInfoMgr
	}

	redisInfoMgrLock.Lock()
	defer redisInfoMgrLock.Unlock()

	if redisInfoMgr != nil {
		return redisInfoMgr
	}

	redisInfoMgr = &RedisInfoMgr{}
	return redisInfoMgr
}

// Insert a RedisInfo
func (mgr *RedisInfoMgr) Insert(info RedisInfo) (err error) {

	db := store.Instance().GetInfoDB()
	c, err := db.Collection(store.RedisInfoCollection)
	if err != nil {
		return
	}

	_, err = c.InsertOne(info, nil)
	if err != nil {
		return
	}

	return

}

// GetAll returns all redis info
func (mgr *RedisInfoMgr) GetAll() (result []RedisInfo, err error) {
	db := store.Instance().GetInfoDB()
	c, err := db.Collection(store.RedisInfoCollection)
	if err != nil {
		return
	}

	err = c.GetAll(&result, nil)
	return
}
