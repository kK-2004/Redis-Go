package database

import (
	"Redis_Go/interface/database"
	"Redis_Go/interface/resp"
	"Redis_Go/lib/utils"
	"Redis_Go/resp/reply"
	"strconv"
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

// execIncrBy increments the stored value by the specified amount
func execIncrBy(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	increment := string(args[1])

	delta, err := strconv.ParseInt(increment, 10, 64)
	if err != nil {
		return reply.GetStandardErrorReply("ERR value is not an integer or out of range")
	}

	entity, ok := db.GetEntity(key)
	var oldValue int64
	if ok {
		if oldStr, ok := entity.Data.([]byte); ok {
			oldValue, err = strconv.ParseInt(string(oldStr), 10, 64)
			if err != nil {
				return reply.GetStandardErrorReply("ERR value is not an integer or out of range")
			}
		}
	}

	newValue := oldValue + delta
	newValueStr := strconv.FormatInt(newValue, 10)
	db.PutEntity(key, &database.DataEntity{Data: []byte(newValueStr)})
	db.addAof(utils.ToCmdLineWithName("INCRBY", args...))
	return reply.GetIntReply(newValue)
}

// execIncr increments the stored value by 1
func execIncr(db *DB, args [][]byte) resp.Reply {
	return execIncrBy(db, [][]byte{args[0], []byte("1")})
}

func init() {
	RegisterCommand("GET", execGet, 2)
	RegisterCommand("SET", execSet, 3)
	RegisterCommand("SETNX", execSetNX, 3)
	RegisterCommand("GETSET", execGetSet, 3)
	RegisterCommand("SETEX", execSet, 4)
	RegisterCommand("STRLEN", execStrLen, 2)
	RegisterCommand("INCR", execIncr, 2)
	RegisterCommand("INCRBY", execIncrBy, 3)
}
