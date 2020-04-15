package txn

import (
	"context"
	"errors"

	"github.com/zhiqiangxu/mondis"
	"github.com/zhiqiangxu/mondis/document/meta/sequence"
	"github.com/zhiqiangxu/mondis/document/schema"
)

// Txn for document db
type Txn struct {
	mondis.ProviderTxn
	handle              *schema.Handle
	startMetaCache      *schema.MetaCache
	sequenceMap         map[int64]*sequence.Hash
	referredCollections map[int64]struct{}
	cancelFuncs         []func()
	update              bool
}

// NewTxn is ctor for Txn
func NewTxn(handle *schema.Handle, update bool, kvdb mondis.KVDB) *Txn {
	startMetaCache := handle.Get()
	t := kvdb.NewTransaction(update)
	return &Txn{ProviderTxn: t, handle: handle, startMetaCache: startMetaCache, update: update}
}

var (
	// ErrDDLConflict used by Txn
	ErrDDLConflict = errors.New("ddl conflict")
)

// Discard Txn
func (txn *Txn) Discard() {
	txn.ProviderTxn.Discard()
}

// Commit Txn
func (txn *Txn) Commit() (err error) {

	if !txn.update {
		return
	}

	defer func() {
		if err != nil {
			for _, cancelFunc := range txn.cancelFuncs {
				cancelFunc()
			}
			txn.cancelFuncs = nil
		}
	}()

	ok, err := txn.handle.Check(context.Background(), txn.startMetaCache, txn.referredCollections)
	if err != nil {
		return
	}

	if !ok {
		err = ErrDDLConflict
		return
	}

	err = txn.ProviderTxn.Commit()
	return
}

// AddCancelFunc adds a cancelFunc to be called when Commit failed
func (txn *Txn) AddCancelFunc(cancelFunc func()) {
	if !txn.update {
		panic("AddCancelFunc called on read only txn")
	}
	txn.cancelFuncs = append(txn.cancelFuncs, cancelFunc)
}

// StartMetaCache returns startMetaCache
func (txn *Txn) StartMetaCache() *schema.MetaCache {
	return txn.startMetaCache
}

// ReferredCollections for storing referred collections before commit
func (txn *Txn) ReferredCollections(collectionIDs ...int64) {
	if txn.referredCollections == nil {
		txn.referredCollections = make(map[int64]struct{})
	}

	for _, collectionID := range collectionIDs {
		txn.referredCollections[collectionID] = struct{}{}
	}
}
