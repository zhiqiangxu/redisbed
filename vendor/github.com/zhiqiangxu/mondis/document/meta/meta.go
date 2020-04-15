package meta

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"

	"errors"

	"github.com/zhiqiangxu/mondis"
	"github.com/zhiqiangxu/mondis/document/keyspace"
	"github.com/zhiqiangxu/mondis/document/model"
	"github.com/zhiqiangxu/mondis/kv"
	"github.com/zhiqiangxu/mondis/kv/numeric"
	"github.com/zhiqiangxu/mondis/structure"
)

// Meta is for handling meta information in a transaction.
type Meta struct {
	txn        *structure.TxStructure
	jobListKey JobListKeyType
}

var (
	globalIDMutex sync.Mutex
)

// Meta structure:
//	schemaVersion -> int64
//  schemaDiff:1 -> schema diff data []byte
//  schemaDiff:2 -> schema diff data []byte
//  bootstrap 	-> int64
//	globalID -> int64
//	dbs -> {
//		db:1 -> db meta data []byte
//		db:2 -> db meta data []byte
//	}
//	db:1 -> {
//		collectionInfo:1 -> collection meta data []byte
//		collectionInfo:2 -> collection meta data []byte
//		didSequence:1 -> int64
//		didSequence:2 -> int64
//	}
//

var (
	schemaVersionKey     = []byte("schemaVersion")
	schemaDiffPrefix     = []byte("schemaDiff")
	bootstrapKey         = []byte("bootstrap")
	globalIDKey          = []byte("globalID")
	dbsKey               = []byte("dbs")
	dbPrefix             = []byte("db")
	collectionInfoPrefix = []byte("collectionInfo")
	didSequencePrefix    = []byte("didSequence")
)

var (
	// ErrDBNotExists used by Meta
	ErrDBNotExists = errors.New("db not exists")
	// ErrDBExists used by Meta
	ErrDBExists = errors.New("db exists")
	// ErrCollectionExists used by Meta
	ErrCollectionExists = errors.New("collection exists")
	// ErrCollectionNotExists used by Meta
	ErrCollectionNotExists = errors.New("collection not exists")
	// ErrJobNotExists used by Meta
	ErrJobNotExists = errors.New("job not exists")
)

// NewMeta creates a Meta in transaction txn.
func NewMeta(txn mondis.ProviderTxn, jobListKeys ...JobListKeyType) *Meta {
	t := structure.New(txn, keyspace.MetaPrefixBytes)
	listKey := DefaultJobListKey
	if len(jobListKeys) != 0 {
		listKey = jobListKeys[0]
	}
	return &Meta{
		txn:        t,
		jobListKey: listKey,
	}
}

func dbKeyByID(dbID int64) []byte {
	return []byte(fmt.Sprintf("%s:%d", dbPrefix, dbID))
}

func (m *Meta) collectionInfoKeyByID(collectionID int64) []byte {
	return []byte(fmt.Sprintf("%s:%d", collectionInfoPrefix, collectionID))
}

func didSequenceKeyByID(collectionID int64) []byte {
	return []byte(fmt.Sprintf("%s:%d", didSequencePrefix, collectionID))
}

func (m *Meta) checkDBExists(dbKey []byte) (err error) {
	_, err = m.txn.HGet(dbsKey, dbKey)
	if err == kv.ErrKeyNotFound {
		err = ErrDBNotExists
	}
	return
}

func (m *Meta) checkDBNotExists(dbKey []byte) (err error) {
	_, err = m.txn.HGet(dbsKey, dbKey)
	if err == kv.ErrKeyNotFound {
		err = nil
	} else if err == nil {
		err = ErrDBExists
	}
	return
}

func (m *Meta) checkCollectionExists(dbKey []byte, collectionKey []byte) (err error) {
	_, err = m.txn.HGet(dbKey, collectionKey)
	if err == kv.ErrKeyNotFound {
		err = ErrCollectionNotExists
	}
	return
}

func (m *Meta) checkCollectionNotExists(dbKey []byte, collectionKey []byte) (err error) {
	_, err = m.txn.HGet(dbKey, collectionKey)
	if err == kv.ErrKeyNotFound {
		err = nil
	} else if err == nil {
		err = ErrCollectionExists
	}
	return
}

// GetGlobalID gets current global id.
func (m *Meta) GetGlobalID() (int64, error) {
	return m.txn.GetInt64(globalIDKey)
}

// GenGlobalID generates next global id.
func (m *Meta) GenGlobalID() (int64, error) {
	return m.txn.Inc(globalIDKey, 1)
}

// GenGlobalIDs for generating multiple ids in (start, end]
func (m *Meta) GenGlobalIDs(n int) (start, end int64, err error) {
	n64 := int64(n)
	end, err = m.txn.Inc(globalIDKey, n64)
	if err != nil {
		return
	}
	start = end - n64
	return
}

// GetSchemaVersion gets current global schema version.
func (m *Meta) GetSchemaVersion() (v int64, err error) {
	v, err = m.txn.GetInt64(schemaVersionKey)
	if err == kv.ErrKeyNotFound {
		err = nil
	}
	return
}

// GenSchemaVersion generates next schema version.
func (m *Meta) GenSchemaVersion() (int64, error) {
	return m.txn.Inc(schemaVersionKey, 1)
}

// CreateDatabase creates a database with db info.
func (m *Meta) CreateDatabase(dbInfo *model.DBInfo) (err error) {
	dbKey := dbKeyByID(dbInfo.ID)

	if err = m.checkDBNotExists(dbKey); err != nil {
		return
	}

	data, err := json.Marshal(dbInfo)
	if err != nil {
		return
	}

	return m.txn.HSet(dbsKey, dbKey, data)
}

// UpdateDatabase updates a database with db info.
func (m *Meta) UpdateDatabase(dbInfo *model.DBInfo) (err error) {
	dbKey := dbKeyByID(dbInfo.ID)

	if err = m.checkDBExists(dbKey); err != nil {
		return
	}

	data, err := json.Marshal(dbInfo)
	if err != nil {
		return
	}

	return m.txn.HSet(dbsKey, dbKey, data)
}

// CreateCollection creates a collection with CollectoinInfo in database.
func (m *Meta) CreateCollection(dbID int64, collectionInfo *model.CollectionInfo) (err error) {
	// Check if db exists.
	dbKey := dbKeyByID(dbID)
	if err = m.checkDBExists(dbKey); err != nil {
		return
	}

	// Check if collection exists.
	collectionInfoKey := m.collectionInfoKeyByID(collectionInfo.ID)
	if err = m.checkCollectionNotExists(dbKey, collectionInfoKey); err != nil {
		return
	}

	data, err := json.Marshal(collectionInfo)
	if err != nil {
		return
	}

	return m.txn.HSet(dbKey, collectionInfoKey, data)
}

// DropDatabase drops whole database.
func (m *Meta) DropDatabase(dbID int64) (err error) {
	// Check if db exists.
	dbKey := dbKeyByID(dbID)
	if err = m.checkDBExists(dbKey); err != nil {
		return
	}

	if err = m.txn.HClear(dbKey); err != nil {
		return
	}

	if err = m.txn.HDel(dbsKey, dbKey); err != nil {
		return
	}

	return
}

// DropCollection drops collection in database.
// If delAutoID is true, it will delete the auto_increment id key-value of the collection.
func (m *Meta) DropCollection(dbID int64, collectionID int64, delAutoID bool) (err error) {
	// Check if db exists.
	dbKey := dbKeyByID(dbID)
	if err = m.checkDBExists(dbKey); err != nil {
		return
	}

	// Check if collection exists.
	collectionInfoKey := m.collectionInfoKeyByID(collectionID)
	if err = m.checkCollectionExists(dbKey, collectionInfoKey); err != nil {
		return
	}

	if err = m.txn.HDel(dbKey, collectionInfoKey); err != nil {
		return
	}
	if delAutoID {
		if err = m.txn.HDel(dbKey, didSequenceKeyByID(collectionID)); err != nil {
			return
		}
	}
	return
}

// UpdateCollection updates the collection with collection info.
func (m *Meta) UpdateCollection(dbID int64, collectionInfo *model.CollectionInfo) (err error) {
	// Check if db exists.
	dbKey := dbKeyByID(dbID)
	if err = m.checkDBExists(dbKey); err != nil {
		return
	}

	// Check if collection exists.
	collectionInfoKey := m.collectionInfoKeyByID(collectionInfo.ID)
	if err = m.checkCollectionExists(dbKey, collectionInfoKey); err != nil {
		return
	}

	data, err := json.Marshal(collectionInfo)
	if err != nil {
		return
	}

	err = m.txn.HSet(dbKey, collectionInfoKey, data)
	return
}

// ListCollections shows all collections in database.
func (m *Meta) ListCollections(dbID int64) (collections []*model.CollectionInfo, err error) {
	dbKey := dbKeyByID(dbID)
	if err = m.checkDBExists(dbKey); err != nil {
		return
	}

	res, err := m.txn.HGetAll(dbKey)
	if err != nil {
		return
	}

	collections = make([]*model.CollectionInfo, 0, len(res)/2)
	for _, r := range res {
		// only handle collection meta
		if !bytes.HasPrefix(r.Field, collectionInfoPrefix) {
			continue
		}

		tbInfo := &model.CollectionInfo{}
		err = json.Unmarshal(r.Value, tbInfo)
		if err != nil {
			return
		}

		collections = append(collections, tbInfo)
	}

	return
}

// ListDatabases shows all databases.
func (m *Meta) ListDatabases() (dbs []*model.DBInfo, err error) {
	res, err := m.txn.HGetAll(dbsKey)
	if err != nil {
		return
	}

	dbs = make([]*model.DBInfo, 0, len(res))
	for _, r := range res {
		dbInfo := &model.DBInfo{}
		err = json.Unmarshal(r.Value, dbInfo)
		if err != nil {
			return
		}
		dbs = append(dbs, dbInfo)
	}
	return
}

// GetDatabase gets the database value with ID.
func (m *Meta) GetDatabase(dbID int64) (dbInfo *model.DBInfo, err error) {
	dbKey := dbKeyByID(dbID)
	value, err := m.txn.HGet(dbsKey, dbKey)
	if err == kv.ErrKeyNotFound {
		err = ErrDBNotExists
	}
	if err != nil {
		return
	}

	dbInfo = &model.DBInfo{}
	err = json.Unmarshal(value, dbInfo)
	return
}

// GetCollection gets the collection value in database with collectionID.
func (m *Meta) GetCollection(dbID int64, collectionID int64) (collectionnfo *model.CollectionInfo, err error) {
	// Check if db exists.
	dbKey := dbKeyByID(dbID)
	if err = m.checkDBExists(dbKey); err != nil {
		return
	}

	collectionInfoKey := m.collectionInfoKeyByID(collectionID)
	value, err := m.txn.HGet(dbKey, collectionInfoKey)
	if err == kv.ErrKeyNotFound {
		err = ErrCollectionNotExists
	}
	if err != nil {
		return
	}

	collectionnfo = &model.CollectionInfo{}
	err = json.Unmarshal(value, collectionnfo)
	return
}

// GetBootstrapVersion returns the version of the server which bootstrap the store.
// If the store is not bootstraped, the version will be zero.
func (m *Meta) GetBootstrapVersion() (ver int64, err error) {
	ver, err = m.txn.GetInt64(bootstrapKey)
	if err == kv.ErrKeyNotFound {
		err = nil
		return
	}
	return
}

// FinishBootstrap finishes bootstrap.
func (m *Meta) FinishBootstrap(version int64) (err error) {
	err = m.txn.SetInt64(bootstrapKey, version)
	return
}

func (m *Meta) schemaDiffKey(schemaVersion int64) []byte {
	return []byte(fmt.Sprintf("%s:%d", schemaDiffPrefix, schemaVersion))
}

// GetSchemaDiff gets the modification information on a given schema version.
func (m *Meta) GetSchemaDiff(schemaVersion int64) (diff *model.SchemaDiff, err error) {
	diffKey := m.schemaDiffKey(schemaVersion)
	data, err := m.txn.Get(diffKey)
	if err != nil {
		if err == kv.ErrKeyNotFound {
			err = nil
		}
		return
	}
	diff = &model.SchemaDiff{}
	err = diff.Decode(data)
	return
}

// SetSchemaDiff sets the modification information on a given schema version.
func (m *Meta) SetSchemaDiff(diff *model.SchemaDiff) (err error) {
	data, err := diff.Encode()
	if err != nil {
		return
	}
	diffKey := m.schemaDiffKey(diff.Version)
	err = m.txn.Set(diffKey, data)
	return
}

// DDL job structure
//	DDLJobList: list jobs
//	DDLJobHistory: hash
//	DDLJobReorg: hash

var (
	ddlJobListKey       = []byte("DDLJobList")
	ddlJobAddIdxListKey = []byte("DDLJobAddIdxList")
	ddlJobHistoryKey    = []byte("DDLJobHistory")
	ddlJobReorgKey      = []byte("DDLJobReorg")
)

// JobListKeyType is a key type of the DDL job queue.
type JobListKeyType []byte

var (
	// DefaultJobListKey keeps all actions of DDL jobs except "add index".
	DefaultJobListKey JobListKeyType = ddlJobListKey
	// AddIndexJobListKey only keeps the action of adding index.
	AddIndexJobListKey JobListKeyType = ddlJobAddIdxListKey
)

func (m *Meta) enQueueDDLJob(key []byte, job *model.Job) (err error) {
	b, err := job.Encode()
	if err == nil {
		err = m.txn.RPush(key, b)
	}
	return
}

// EnQueueDDLJob adds a DDL job to the list.
func (m *Meta) EnQueueDDLJob(job *model.Job, jobListKeys ...JobListKeyType) error {
	listKey := m.jobListKey
	if len(jobListKeys) != 0 {
		listKey = jobListKeys[0]
	}

	return m.enQueueDDLJob(listKey, job)
}

func (m *Meta) deQueueDDLJob(key []byte) (job *model.Job, err error) {
	value, err := m.txn.LPop(key)
	if err == kv.ErrKeyNotFound {
		err = nil
		return
	}
	if err != nil {
		return
	}

	job = &model.Job{}
	err = job.Decode(value)
	return
}

// DeQueueDDLJob pops a DDL job from the list.
func (m *Meta) DeQueueDDLJob() (*model.Job, error) {
	return m.deQueueDDLJob(m.jobListKey)
}

func (m *Meta) getDDLJob(key []byte, index int64) (job *model.Job, err error) {
	value, err := m.txn.LIndex(key, index)
	if err == kv.ErrKeyNotFound {
		err = nil
		return
	}
	if err != nil {
		return
	}

	job = &model.Job{}
	err = job.Decode(value)
	return
}

// GetDDLJobByIdx returns the corresponding DDL job by the index.
func (m *Meta) GetDDLJobByIdx(index int64, jobListKeys ...JobListKeyType) (job *model.Job, err error) {
	listKey := m.jobListKey
	if len(jobListKeys) != 0 {
		listKey = jobListKeys[0]
	}

	job, err = m.getDDLJob(listKey, index)

	return
}

// updateDDLJob updates the DDL job with index and key.
func (m *Meta) updateDDLJob(index int64, job *model.Job, key []byte) (err error) {
	b, err := job.Encode()
	if err == nil {
		err = m.txn.LSet(key, index, b)
		if err == kv.ErrKeyNotFound {
			err = ErrJobNotExists
			return
		}
	}
	return
}

// UpdateDDLJob updates the DDL job with index.
// updateRawArgs is used to determine whether to update the raw args when encode the job.
func (m *Meta) UpdateDDLJob(index int64, job *model.Job, jobListKeys ...JobListKeyType) (err error) {
	listKey := m.jobListKey
	if len(jobListKeys) != 0 {
		listKey = jobListKeys[0]
	}

	err = m.updateDDLJob(index, job, listKey)
	return
}

// DDLJobQueueLen returns the DDL job queue length.
func (m *Meta) DDLJobQueueLen(jobListKeys ...JobListKeyType) (int64, error) {
	listKey := m.jobListKey
	if len(jobListKeys) != 0 {
		listKey = jobListKeys[0]
	}
	return m.txn.LLen(listKey)
}

// GetAllDDLJobsInQueue gets all DDL Jobs in the current queue.
func (m *Meta) GetAllDDLJobsInQueue(jobListKeys ...JobListKeyType) (jobs []*model.Job, err error) {
	listKey := m.jobListKey
	if len(jobListKeys) != 0 {
		listKey = jobListKeys[0]
	}

	values, err := m.txn.LGetAll(listKey)
	if err != nil || len(values) == 0 {
		return
	}

	jobs = make([]*model.Job, 0, len(values))
	for _, val := range values {
		job := &model.Job{}
		err = job.Decode(val)
		if err != nil {
			return
		}
		jobs = append(jobs, job)
	}

	return
}

func (m *Meta) historyJobIDKey(id int64) []byte {
	return numeric.Encode2Binary(uint64(id), nil)
}

// AddHistoryDDLJob adds DDL job to history.
func (m *Meta) AddHistoryDDLJob(job *model.Job) (err error) {
	b, err := job.Encode()
	if err == nil {
		err = m.txn.HSet(ddlJobHistoryKey, m.historyJobIDKey(job.ID), b)
	}
	return
}

// GetHistoryDDLJob gets a history DDL job.
func (m *Meta) GetHistoryDDLJob(id int64) (job *model.Job, err error) {
	value, err := m.txn.HGet(ddlJobHistoryKey, m.historyJobIDKey(id))
	if err == kv.ErrKeyNotFound {
		err = nil
		return
	}
	if err != nil {
		return
	}

	job = &model.Job{}
	err = job.Decode(value)
	return
}

func decodeJobs(jobPairs []structure.HashPair) (jobs []*model.Job, err error) {
	jobs = make([]*model.Job, 0, len(jobPairs))
	for _, pair := range jobPairs {
		job := &model.Job{}
		err = job.Decode(pair.Value)
		if err != nil {
			return
		}
		jobs = append(jobs, job)
	}
	return
}

// GetAllHistoryDDLJobs gets all history DDL jobs.
func (m *Meta) GetAllHistoryDDLJobs() (jobs []*model.Job, err error) {
	pairs, err := m.txn.HGetAll(ddlJobHistoryKey)
	if err != nil {
		return
	}
	jobs, err = decodeJobs(pairs)

	return
}

// GetLastNHistoryDDLJobs gets latest N history ddl jobs.
func (m *Meta) GetLastNHistoryDDLJobs(num int) (jobs []*model.Job, err error) {
	pairs, err := m.txn.HGetNDesc(ddlJobHistoryKey, num)
	if err != nil {
		return
	}
	jobs, err = decodeJobs(pairs)
	return
}

func (m *Meta) reorgJobStartHandle(id int64) []byte {
	return numeric.Encode2Binary(uint64(id), nil)
}

func (m *Meta) reorgJobEndHandle(id int64) []byte {
	b := make([]byte, 0, 12)
	b = numeric.Encode2Binary(uint64(id), b)
	b = append(b, "_end"...)
	return b
}

// UpdateDDLReorgStartHandle saves the job reorganization latest processed start handle for later resuming.
func (m *Meta) UpdateDDLReorgStartHandle(job *model.Job, startHandle int64) (err error) {
	err = m.txn.HSet(ddlJobReorgKey, m.reorgJobStartHandle(job.ID), numeric.Encode2Human(startHandle))
	return
}

// UpdateDDLReorgHandle saves the job reorganization latest processed information for later resuming.
func (m *Meta) UpdateDDLReorgHandle(job *model.Job, startHandle, endHandle, physicalTableID int64) (err error) {
	err = m.txn.HSet(ddlJobReorgKey, m.reorgJobStartHandle(job.ID), numeric.Encode2Human(startHandle))
	if err != nil {
		return
	}
	err = m.txn.HSet(ddlJobReorgKey, m.reorgJobEndHandle(job.ID), numeric.Encode2Human(endHandle))
	return
}

// RemoveDDLReorgHandle removes the job reorganization related handles.
func (m *Meta) RemoveDDLReorgHandle(job *model.Job) (err error) {
	err = m.txn.HDel(ddlJobReorgKey, m.reorgJobStartHandle(job.ID))
	if err != nil {
		return
	}
	err = m.txn.HDel(ddlJobReorgKey, m.reorgJobEndHandle(job.ID))
	return
}

// GetDDLReorgHandle gets the latest processed DDL reorganize position.
func (m *Meta) GetDDLReorgHandle(job *model.Job) (startHandle, endHandle int64, err error) {
	startHandle, err = m.txn.HGetInt64(ddlJobReorgKey, m.reorgJobStartHandle(job.ID))
	if err != nil {
		return
	}
	endHandle, err = m.txn.HGetInt64(ddlJobReorgKey, m.reorgJobEndHandle(job.ID))
	if err != nil {
		return
	}
	return
}
