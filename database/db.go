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

func NewDB(dbIndex ...int) *DB {
	idx := 0
	if len(dbIndex) > 0 {
		idx = dbIndex[0]
	}

	return &DB{
		index: idx,
		data:  dict.GetSyncDict(),
		addAof: func(line CmdLine) {
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
// 注意：调用方必须先持有该 key 的锁
func (db *DB) Remove(key string) int {
	result := db.data.Remove(key)
	if result > 0 {
		db.lockMgr.RemoveLock(key)
	}
	return result
}

// Removes deletes the DataEntity associated with the given keys from the database
// 注意：调用方必须先持有这些 key 的锁
func (db *DB) Removes(keys ...string) int {
	deleted := 0
	for _, key := range keys {
		result := db.data.Remove(key)
		if result > 0 {
			deleted++
			db.lockMgr.RemoveLock(key)
		}
	}
	return deleted
}

// Flush clears the database by removing all DataEntity objects
func (db *DB) Flush() {
	db.data.Clear()
	db.lockMgr.Clear()
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

// KeyLockManager 键级锁管理器
// 使用 sync.Mutex 保护 map 和 refCount 的原子操作
type KeyLockManager struct {
	mu    sync.Mutex
	locks map[string]*keyLockEntry
}

type keyLockEntry struct {
	lock            sync.RWMutex
	refCount        int  // 正在等待或持有锁的 goroutine 数量
	pendingDeletion bool // 标记是否待删除
}

type KeyLockHandle struct {
	key   string
	entry *keyLockEntry
}

func NewKeyLockManager() *KeyLockManager {
	return &KeyLockManager{
		locks: make(map[string]*keyLockEntry),
	}
}

// acquireEntry 原子地获取或创建 entry 并增加引用计数
func (klm *KeyLockManager) acquireEntry(key string) *keyLockEntry {
	klm.mu.Lock()
	defer klm.mu.Unlock()

	entry, ok := klm.locks[key]
	if !ok {
		entry = &keyLockEntry{}
		klm.locks[key] = entry
	}
	entry.refCount++
	return entry
}

// releaseEntry 释放引用，如果 refCount=0 且待删除则清理
func (klm *KeyLockManager) releaseEntry(key string, entry *keyLockEntry) {
	klm.mu.Lock()
	defer klm.mu.Unlock()

	if entry.refCount > 0 {
		entry.refCount--
	}
	if entry.refCount == 0 && entry.pendingDeletion {
		if current, ok := klm.locks[key]; ok && current == entry {
			delete(klm.locks, key)
		}
	}
}

// Lock 获取指定 key 的写锁
func (klm *KeyLockManager) Lock(key string) *KeyLockHandle {
	entry := klm.acquireEntry(key)
	entry.lock.Lock()
	return &KeyLockHandle{key: key, entry: entry}
}

// Unlock 释放写锁
func (klm *KeyLockManager) Unlock(handle *KeyLockHandle) {
	if handle == nil || handle.entry == nil {
		return
	}
	handle.entry.lock.Unlock()
	klm.releaseEntry(handle.key, handle.entry)
}

// RLock 获取指定 key 的读锁
func (klm *KeyLockManager) RLock(key string) *KeyLockHandle {
	entry := klm.acquireEntry(key)
	entry.lock.RLock()
	return &KeyLockHandle{key: key, entry: entry}
}

// RUnlock 释放读锁
func (klm *KeyLockManager) RUnlock(handle *KeyLockHandle) {
	if handle == nil || handle.entry == nil {
		return
	}
	handle.entry.lock.RUnlock()
	klm.releaseEntry(handle.key, handle.entry)
}

// RemoveLock 标记删除指定 key 的锁（在 key 被删除后调用）
// 如果 refCount=0 则立即删除，否则标记为待删除，等待所有等待者完成后删除
func (klm *KeyLockManager) RemoveLock(key string) {
	klm.mu.Lock()
	defer klm.mu.Unlock()

	entry, ok := klm.locks[key]
	if !ok {
		return
	}
	if entry.refCount == 0 {
		delete(klm.locks, key)
		return
	}
	entry.pendingDeletion = true
}

// Clear 清空所有锁
func (klm *KeyLockManager) Clear() {
	klm.mu.Lock()
	defer klm.mu.Unlock()

	for key, entry := range klm.locks {
		if entry.refCount == 0 {
			delete(klm.locks, key)
			continue
		}
		entry.pendingDeletion = true
	}
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
