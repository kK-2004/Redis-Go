package database

import (
	"Redis_Go/aof"
	"Redis_Go/cluster"
	"Redis_Go/config"
	DatabaseInterface "Redis_Go/interface/database"
	"Redis_Go/interface/resp"
	"Redis_Go/lib/logger"
	"Redis_Go/resp/reply"
	"strconv"
	"strings"
)

type Database struct {
	dbSet      []DatabaseInterface.Database
	aofHandler *aof.AofHandler
}

func CreateDatabases(args ...string) DatabaseInterface.Database {
	// Cluster mode
	if config.Properties.UseCluster {
		logger.Info("Starting in cluster mode")
		db := NewDB(0)
		clusterDB := cluster.NewClusterDatabase(db)

		// Initialize AOF for cluster mode
		if config.Properties.AppendOnly {
			// Create a wrapper Database for AOF handler
			wrapper := &Database{
				dbSet: []DatabaseInterface.Database{db},
			}
			aofHandler, err := aof.NewAofHandler(wrapper)
			if err != nil {
				panic(err)
			}
			// Set addAof callback for the single DB
			db.addAof = func(line CmdLine) {
				aofHandler.AddAof(0, line)
			}
		}
		return clusterDB
	}

	// Standalone mode
	if config.Properties.Databases <= 0 {
		config.Properties.Databases = 16
	}
	databases := &Database{
		dbSet: make([]DatabaseInterface.Database, config.Properties.Databases),
	}
	// 根据参数初始化不同类型的数据库
	if len(args) == 1 {
		switch args[0] {
		case "echo_database":
			for i := range databases.dbSet {
				databases.dbSet[i] = NewEchoDatabase()
			}
		default:
			for i := range databases.dbSet {
				databases.dbSet[i] = NewDB(i)
			}
		}
	} else {
		// 默认创建普通数据库
		for i := range databases.dbSet {
			databases.dbSet[i] = NewDB(i)
		}
	}
	if config.Properties.AppendOnly {
		aofHandler, err := aof.NewAofHandler(databases)
		if err != nil {
			panic(err)
		}
		databases.aofHandler = aofHandler

		// 为每个db实例，添加addAof方法
		for _, db := range databases.dbSet {
			sdb := db
			if sdb, ok := sdb.(*DB); ok {
				sdb.addAof = func(line CmdLine) {
					aofHandler.AddAof(sdb.index, line)
				}
			}
		}
	}
	return databases
}

func execSelect(c resp.Connection, database *Database, args [][]byte) resp.Reply {
	dbIndex, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return reply.GetStandardErrorReply("ERR invalid DB index")
	}
	if dbIndex < 0 || dbIndex >= len(database.dbSet) {
		return reply.GetStandardErrorReply("ERR DB index is out of range")
	}
	c.SelectDB(dbIndex)
	return reply.GetStatusReply("OK")
}

func (d *Database) Exec(client resp.Connection, args [][]byte) (result resp.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error("Database Exec panic:" + err.(error).Error())
			result = reply.GetStandardErrorReply("ERR internal error: " + err.(error).Error())
		}
	}()
	cmdName := strings.ToLower(string(args[0]))

	// 处理 SELECT 命令
	if cmdName == "select" {
		if len(args) != 2 {
			return reply.GetArgNumErrReply("select")
		}
		return execSelect(client, d, args[1:])
	}

	// 执行命令
	db := d.dbSet[client.GetDBIndex()]
	return db.Exec(client, args)
}

// AfterClientClose 在客户端连接关闭后调用
func (d *Database) AfterClientClose(c resp.Connection) {
	// 清理客户端相关资源（如果需要）
}

// Close 关闭所有数据库
func (d *Database) Close() {
	for _, db := range d.dbSet {
		db.Close()
	}
}
