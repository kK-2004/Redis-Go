package connection

import (
	"Redis_Go/lib/sync/wait"
	"net"
	"sync"
	"time"
)

// 应用层Connection

type Connection struct {
	conn         net.Conn
	waitingReply wait.Wait
	mu           sync.Mutex
	selectedDB   int
	dbSelected   bool // 是否已显式选择数据库
}

func NewConnection(conn net.Conn) *Connection {
	return &Connection{
		conn:       conn,
		dbSelected: false, // 初始状态为未选择数据库
	}
}

func (c *Connection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *Connection) Close() error {
	c.waitingReply.WaitWithTimeout(10 * time.Second)
	_ = c.conn.Close()
	return nil
}

func (c *Connection) Write(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	c.mu.Lock()
	c.waitingReply.Add(1)
	defer func() {
		c.waitingReply.Done()
		c.mu.Unlock()
	}()

	_, err := c.conn.Write(b)
	return err
}

func (c *Connection) GetDBIndex() int {
	return c.selectedDB
}

func (c *Connection) SelectDB(index int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.selectedDB = index
	c.dbSelected = true // 标记为已选择数据库
}

func (c *Connection) GetDBSelected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dbSelected
}
