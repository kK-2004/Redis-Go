package database

import (
	"Redis_Go/interface/resp"
	"Redis_Go/resp/reply"
)

func Ping(db *DB, args [][]byte) resp.Reply {
	return reply.GetPongReply()
}

func init() {
	RegisterCommand("ping", Ping, 1)
}
