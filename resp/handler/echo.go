package handler

import (
	"Redis_Go/lib/logger"
	"Redis_Go/lib/sync/wait"
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// EchoHandler 负责管理所有活动的客户端连接
type EchoHandler struct {
	activeConn sync.Map
	closing    atomic.Bool
}

// EchoClient 管理单个客户端连接
type EchoClient struct {
	Conn    net.Conn
	Waiting wait.Wait
}

func GetEchoHandler() *EchoHandler {
	return &EchoHandler{}
}

func (c *EchoClient) Close() error {
	timeOut := c.Waiting.WaitWithTimeout(10 * time.Second)
	err := c.Conn.Close()
	if err != nil {
		return err
	}
	if timeOut {
		return errors.New("echo client close timeout")
	}
	return nil
}

func (h *EchoHandler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Load() {
		_ = conn.Close()
	}

	client := &EchoClient{
		Conn: conn,
	}

	h.activeConn.Store(client, struct{}{})

	reader := bufio.NewReader(conn)
	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				logger.Info("client close")
				h.activeConn.Delete(client)
			} else {
				logger.Warn(err)
			}
			return
		}
		client.Waiting.Add(1)
		logger.Info("[Echo] received: %s", msg)
		b := []byte("Redis_Go received: " + msg)
		_, _ = conn.Write(b)
		client.Waiting.Done()
	}
}

func (h *EchoHandler) Close() error {
	logger.Info("handler shutting down...")
	h.closing.Store(true)
	h.activeConn.Range(func(key, value interface{}) bool {
		client := key.(*EchoClient)
		err := client.Close()
		if err != nil {
			logger.Error(err)
			return false
		}
		return true
	})
	return nil
}
