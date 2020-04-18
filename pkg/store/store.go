package store

import (
	"strconv"
	"sync"

	"path"

	"context"
	"os"

	"github.com/zhiqiangxu/mondis"
	"github.com/zhiqiangxu/mondis/document/ddl"
	"github.com/zhiqiangxu/mondis/document/dml"
	"github.com/zhiqiangxu/mondis/document/domain"
	"github.com/zhiqiangxu/mondis/kv"
	"github.com/zhiqiangxu/mondis/provider"
	"github.com/zhiqiangxu/mondis/structure"
	"github.com/zhiqiangxu/redisbed/pkg/config"
	"github.com/zhiqiangxu/redisbed/pkg/logger"
	"go.uber.org/zap"
)

// Store maintains all redis info
type Store struct {
	sync.RWMutex
	redisDB map[uint16]mondis.KVDB

	kvdb mondis.KVDB
	do   *domain.Domain
	db   *dml.DB
}

const (
	infoDir  = "info"
	redisDir = "redis"

	// RedisInfoCollection for redis info
	RedisInfoCollection = "redis_info"

	metaPrefix = "meta"
	dbName     = "rb"
	versionKey = "rbv"
)

var (
	store     *Store
	storeLock sync.Mutex
)

// Instance is singleton for Store
func Instance() *Store {
	if store != nil {
		return store
	}

	storeLock.Lock()
	defer storeLock.Unlock()

	if store != nil {
		return store
	}

	conf := config.Load()
	kvdb := provider.NewBadger()
	infoPath := path.Join(conf.DataDir, infoDir)
	err := os.MkdirAll(infoPath, 0777)
	if err != nil {
		logger.Instance().Fatal("MkdirAll", zap.String("infoPath", infoPath), zap.Error(err))
	}
	err = kvdb.Open(mondis.KVOption{Dir: infoPath})
	if err != nil {
		logger.Instance().Fatal("kvdb.Open", zap.Error(err))
	}
	do := domain.NewDomain(kvdb)

	err = do.Init()
	if err != nil {
		kvdb.Close()
		logger.Instance().Fatal("do.Init", zap.Error(err))
	}

	store = &Store{kvdb: kvdb, do: do, redisDB: make(map[uint16]mondis.KVDB)}

	return store
}

// GetInfoDB returns the db for model
func (m *Store) GetInfoDB() *dml.DB {
	return m.db
}

// GetRedisDB returns KVDB for redis
func (m *Store) GetRedisDB(port uint16) mondis.KVDB {
	m.RLock()
	kvdb := m.redisDB[port]
	m.RUnlock()
	if kvdb != nil {
		return kvdb
	}

	m.Lock()
	defer m.Unlock()
	kvdb = m.redisDB[port]
	if kvdb != nil {
		return kvdb
	}

	return m.createRedisDBLocked(port)
}

func (m *Store) createRedisDBLocked(port uint16) mondis.KVDB {
	kvdb := provider.NewBadger()
	conf := config.Load()
	redisPath := path.Join(conf.DataDir, redisDir, strconv.Itoa(int(port)))
	err := os.MkdirAll(redisPath, 0777)
	if err != nil {
		logger.Instance().Fatal("MkdirAll", zap.String("redisPath", redisPath), zap.Error(err))
	}
	err = kvdb.Open(mondis.KVOption{Dir: redisPath})
	if err != nil {
		logger.Instance().Fatal("kvdb.Open", zap.Error(err))
	}

	m.redisDB[port] = kvdb

	return kvdb
}

// Finalize meta
func (m *Store) Finalize() (err error) {
	err = m.kvdb.Close()
	if err != nil {
		return
	}

	m.Lock()
	defer m.Unlock()

	for _, kvdb := range m.redisDB {
		e := kvdb.Close()
		if e != nil {
			err = e
		}
	}
	return
}

// Bootstrap storage
func (m *Store) Bootstrap() (err error) {
	err = m.bootstrap()
	if err != nil {
		return
	}

	db, err := m.do.DB(dbName)
	if err != nil {
		return
	}
	m.db = db

	return
}

func (m *Store) bootstrap() (err error) {

	txn := m.kvdb.NewTransaction(true)
	defer txn.Discard()

	txStruct := structure.New(txn, []byte(metaPrefix))
	v, err := txStruct.GetInt64([]byte(versionKey))
	if err == kv.ErrKeyNotFound {
		err = nil
	}
	if err != nil {
		return
	}

	switch v {
	case 0:
		_, err = m.do.DDL().CreateSchema(context.Background(), ddl.CreateSchemaInput{DB: dbName, Collections: []string{RedisInfoCollection}})
		if err != nil {
			return
		}
		txStruct.SetInt64([]byte(versionKey), 1)
		err = txn.Commit()
	case 1:
	}
	return
}
