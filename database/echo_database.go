package database

import (
	"Redis_Go/interface/resp"
	"Redis_Go/lib/logger"
	"Redis_Go/resp/reply"
)

type EchoDatabase struct {
}

func NewEchoDatabase() *EchoDatabase {
	return &EchoDatabase{}
}

func (e *EchoDatabase) Exec(client resp.Connection, args [][]byte) resp.Reply {
	return reply.GetMultiBulkReply(args)
}

func (e *EchoDatabase) AfterClientClose(c resp.Connection) {
	logger.Info("EchoDatabase AfterClientClose")
}

func (e *EchoDatabase) Close() {
	logger.Info("EchoDatabase Close")
}
