package database

import "Redis_Go/interface/resp"

type CmdLine = [][]byte

type Database interface {
	Exec(client resp.Connection, args CmdLine) resp.Reply
	AfterClientClose(c resp.Connection)
	Close()
}

type DataEntity struct {
	Data interface{}
}
