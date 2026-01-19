package database

import (
	"Redis_Go/datastruct/dict"
	"Redis_Go/interface/database"
	"Redis_Go/interface/resp"
	"Redis_Go/resp/reply"
	"strings"
)

type DB struct {
	index  int
	data   dict.Dict
	addAof func(CmdLine)
}

func NewDB(index ...int) *DB {
	if len(index) > 0 {
		return &DB{
			index: index[0],
			data:  dict.GetSyncDict(),
			addAof: func(line CmdLine) {

			},
		}
	}
	return &DB{
		index: 0,
		data:  dict.GetSyncDict(),
		addAof: func(line CmdLine) {
			// No-op by default,
			// can be overridden by the database instance
		},
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
	return db.data.Remove(key)
}

// Removes deletes the DataEntity associated with the given keys from the database
func (db *DB) Removes(keys ...string) int {
	deleted := 0
	for _, key := range keys {
		// Use Remove's return value directly to avoid race condition between Get and Remove
		result := db.data.Remove(key)
		if result > 0 {
			deleted++
		}
	}
	return deleted
}

// Flush clears the database by removing all DataEntity objects
func (db *DB) Flush() {
	db.data.Clear()
}

// AfterClientClose is called when a client connection is closed
func (db *DB) AfterClientClose(c resp.Connection) {
	// TODO: cleanup client-specific resources if needed
}

// Close closes the database and releases resources
func (db *DB) Close() {
	db.data.Clear()
}
