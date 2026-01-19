package database

import (
	"Redis_Go/interface/database"
	"Redis_Go/interface/resp"
	"Redis_Go/lib/utils"
	"Redis_Go/resp/reply"
)

// execGet retrieves the value associated with the specified key from the database.
func execGet(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	if entity, ok := db.GetEntity(key); ok {
		// TODO: If we have multiple types, we need to check the conversion if it's not []byte
		return reply.GetBulkReply(entity.Data.([]byte))
	}
	return reply.GetNullBulkReply()
}

func execSet(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	value := args[1]
	entity := &database.DataEntity{
		Data: value,
	}
	db.PutEntity(key, entity)
	db.addAof(utils.ToCmdLineWithName("SET", args...))
	return reply.GetStatusReply("OK")
}

func execSetNX(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	value := args[1]
	entity := &database.DataEntity{
		Data: value,
	}
	result := db.PutIfAbsent(key, entity)
	db.addAof(utils.ToCmdLineWithName("SETNX", args...))
	return reply.GetIntReply(int64(result))
}

func execGetSet(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	value := args[1]

	entity, ok := db.GetEntity(key)
	db.PutEntity(key, &database.DataEntity{
		Data: value,
	})
	db.addAof(utils.ToCmdLineWithName("GETSET", args...))
	if !ok {
		return reply.GetNullBulkReply()
	}
	return reply.GetBulkReply(entity.Data.([]byte))
}

func execStrLen(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	entity, ok := db.GetEntity(key)
	if !ok {
		return reply.GetNullBulkReply()
	}
	return reply.GetIntReply(int64(len(entity.Data.([]byte))))
}

func init() {
	RegisterCommand("GET", execGet, 2)
	RegisterCommand("SET", execSet, 3)
	RegisterCommand("SETNX", execSetNX, 3)
	RegisterCommand("GETSET", execGetSet, 3)
	RegisterCommand("SETEX", execSet, 4)
	RegisterCommand("STRLEN", execStrLen, 2)
}
