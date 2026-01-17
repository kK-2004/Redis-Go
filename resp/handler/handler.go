package handler

import (
	"Redis_Go/database"
	databaseface "Redis_Go/interface/database"
	"Redis_Go/lib/logger"
	"Redis_Go/resp/connection"
	"Redis_Go/resp/parser"
	"Redis_Go/resp/reply"
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
)

var unknownErrReplyBytes = []byte("-ERR unknown\r\n")

type RespHandler struct {
	activeConn sync.Map
	db         databaseface.Database
	closing    atomic.Bool
}

func GetRespHandler() *RespHandler {
	db := database.NewEchoDatabase()
	return &RespHandler{
		db: db,
	}
}

func (h *RespHandler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Load() {
		_ = conn.Close()
	}

	client := connection.NewConnection(conn)
	h.activeConn.Store(client, 1)

	ch := parser.ParseStream(conn)
	for payload := range ch {
		if payload.Err != nil {
			if payload.Err == io.EOF ||
				payload.Err == io.ErrUnexpectedEOF ||
				strings.Contains(payload.Err.Error(), "use of closed network connection") {
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}
			continue
		}
		if payload.Data == nil {
			logger.Error("empty payload")
			continue
		}
		r, ok := payload.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk reply")
			continue
		}
		result := h.db.Exec(client, r.Args)
		if result != nil {
			_ = client.Write(result.ToBytes())
		} else {
			_ = client.Write(unknownErrReplyBytes)
		}
	}
}

func (h *RespHandler) closeClient(client *connection.Connection) {
	_ = client.Close()
	h.db.AfterClientClose(client)
	h.activeConn.Delete(client)
}

func (h *RespHandler) Close() error {
	logger.Info("handler shutting down...")
	h.closing.Store(true)
	// TODO: close after completed
	h.activeConn.Range(func(key, value interface{}) bool {
		client := key.(*connection.Connection)
		_ = client.Close()
		return true
	})
	h.db.Close()
	return nil
}
