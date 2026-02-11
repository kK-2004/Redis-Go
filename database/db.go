package database

import (
	"Redis_Go/datastruct/dict"
	"Redis_Go/datastruct/hash"
	"Redis_Go/datastruct/zset"
	"Redis_Go/interface/database"
	"Redis_Go/interface/resp"
	"Redis_Go/resp/reply"
	"strings"
	"sync"
)

type DB struct {
	index   int
	data    dict.Dict
	addAof  func(CmdLine)
	lockMgr *KeyLockManager
}

func NewDB(index ...int) *DB {
	return &DB{
		index: 0,
		data:  dict.GetSyncDict(),
		addAof: func(line CmdLine) {
			// No-op by default,
			// can be overridden by the database instance
		},
		lockMgr: NewKeyLockManager(),
	}
}

type ExecFunc func(db *DB, args [][]byte) resp.Reply

type CmdLine = [][]byte

func (db *DB) Exec(c resp.Connection, cmdLine CmdLine) resp.Reply {
	cmdName := strings.ToLower(string((cmdLine[0])))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return reply.GetStandardErrorReply("ERR unknown command '" + cmdName + "'")
	}
	if !validateArgCnt(cmd.argCnt, cmdLine) {
		return reply.GetArgNumErrReply(cmdName)
	}
	return cmd.exec(db, cmdLine[1:])
}

func validateArgCnt(argCnt int, args [][]byte) bool {
	if argCnt >= 0 {
		return len(args) == argCnt
	} else { // If the arity is negative, it means the command takes a variable number of arguments
		// Check if the number of arguments is within the valid range
		return len(args) >= -argCnt
	}
}

// GetEntity returns DataEntity bind to the given key
func (db *DB) GetEntity(key string) (*database.DataEntity, bool) {
	raw, ok := db.data.Get(key)
	if !ok {
		return nil, false
	}
	entity, _ := raw.(*database.DataEntity)
	return entity, true
}

// PutEntity stores the given DataEntity in the database
func (db *DB) PutEntity(key string, entity *database.DataEntity) int {
	return db.data.Put(key, entity)
}

// PutIfExists edit the given DataEntity in the database
func (db *DB) PutIfExists(key string, entity *database.DataEntity) int {
	return db.data.PutIfExists(key, entity)
}

// PutIfAbsent stores the given DataEntity in the database if it doesn't already exist
func (db *DB) PutIfAbsent(key string, entity *database.DataEntity) int {
	return db.data.PutIfAbsent(key, entity)
}

// Remove deletes the DataEntity associated with the given key from the database
func (db *DB) Remove(key string) int {
	result := db.data.Remove(key)
	if result > 0 {
		db.lockMgr.CleanupLock(key)
	}
	return result
}

// Removes deletes the DataEntity associated with the given keys from the database
func (db *DB) Removes(keys ...string) int {
	deleted := 0
	for _, key := range keys {
		// Use Remove's return value directly to avoid race condition between Get and Remove
		result := db.data.Remove(key)
		if result > 0 {
			deleted++
			db.lockMgr.CleanupLock(key)
		}
	}
	return deleted
}

// Flush clears the database by removing all DataEntity objects
func (db *DB) Flush() {
	db.data.Clear()
	db.lockMgr.locks = sync.Map{}
}

// AfterClientClose is called when a client connection is closed
func (db *DB) AfterClientClose(c resp.Connection) {
	// TODO: cleanup client-specific resources if needed
}

// Close closes the database and releases resources
func (db *DB) Close() {
	db.data.Clear()
}

// getAsHash returns a hash value stored at key, or nil if it doesn't exist
func (db *DB) getAsHash(key string) (*hash.Hash, bool) {
	entity, exists := db.GetEntity(key)
	if !exists {
		return nil, false
	}

	hashObj, ok := entity.Data.(*hash.Hash)
	if !ok {
		return nil, true // key exists but not a hash
	}
	return hashObj, true
}

// getOrCreateHash gets or creates a hash
func (db *DB) getOrCreateHash(key string) (*hash.Hash, bool) {
	hashObj, exists := db.getAsHash(key)
	if exists {
		return hashObj, true
	}

	// Create a new hash
	hashObj = hash.MakeHash()
	db.PutEntity(key, &database.DataEntity{Data: hashObj})
	return hashObj, false
}

func getAsZSet(db *DB, key string) (zset.ZSet, bool) {
	// Get entity from database
	entity, exists := db.GetEntity(key)
	if !exists {
		return zset.NewZSet(), false
	}

	// Check if entity is a ZSet
	zsetObj, ok := entity.Data.(zset.ZSet)
	if !ok {
		return nil, true // Key exists but is not a ZSet
	}

	return zsetObj, true
}

type KeyLockManager struct {
	locks sync.Map
}

func NewKeyLockManager() *KeyLockManager {
	return &KeyLockManager{}
}

func (klm *KeyLockManager) Lock(key string) *sync.RWMutex {
	lockInterface, _ := klm.locks.LoadOrStore(key, &sync.RWMutex{})
	lock := lockInterface.(*sync.RWMutex)
	lock.Lock()
	return lock
}

func (klm *KeyLockManager) Unlock(lock *sync.RWMutex) {
	if lock == nil {
		return
	}
	lock.Unlock()
}

// RLock acquires a read lock for the given key
func (klm *KeyLockManager) RLock(key string) *sync.RWMutex {
	lockInterface, _ := klm.locks.LoadOrStore(key, &sync.RWMutex{})
	lock := lockInterface.(*sync.RWMutex)
	lock.RLock()
	return lock
}

// RUnlock releases a read lock for the given key
func (klm *KeyLockManager) RUnlock(lock *sync.RWMutex) {
	if lock == nil {
		return
	}
	lock.RUnlock()
}

func (klm *KeyLockManager) CleanupLock(key string) {
	klm.locks.Delete(key)
}

func (db *DB) WithKeyLock(key string, fn func()) {
	lock := db.lockMgr.Lock(key)
	defer db.lockMgr.Unlock(lock)
	fn()
}

func (db *DB) WithRKeyLock(key string, fn func()) {
	lock := db.lockMgr.RLock(key)
	defer db.lockMgr.RUnlock(lock)
	fn()
}

func (db *DB) WithKeyLockReturn(key string, fn func() interface{}) interface{} {
	lock := db.lockMgr.Lock(key)
	defer db.lockMgr.Unlock(lock)
	return fn()
}
