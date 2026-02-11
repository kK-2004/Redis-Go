package aof

import (
	"Redis_Go/config"
	databaseface "Redis_Go/interface/database"
	"Redis_Go/lib/logger"
	"Redis_Go/lib/utils"
	"Redis_Go/resp/connection"
	"Redis_Go/resp/parser"
	"Redis_Go/resp/reply"
	"io"
	"os"
	"strconv"
	"strings"
)

type cmdLine = [][]byte

type payload struct {
	cmdLine cmdLine
	dbIndex int
}

type AofHandler struct {
	db          databaseface.Database
	aofChan     chan *payload
	aofFile     *os.File
	aofFilename string
	currentDB   int
}

const aofBufferSize = 1 << 16

func NewAofHandler(db databaseface.Database) (*AofHandler, error) {
	handler := &AofHandler{
		db:          db,
		aofFilename: config.Properties.AppendOnlyFilename,
	}
	handler.loadAof()
	aofFile, err := os.OpenFile(handler.aofFilename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}
	handler.aofFile = aofFile
	handler.aofChan = make(chan *payload, aofBufferSize)
	handler.currentDB = 0
	go func() {
		handler.handleAof()
	}()
	return handler, nil
}

func (h *AofHandler) AddAof(dbIndex int, cmdLine cmdLine) {
	if !config.Properties.AppendOnly {
		return
	}
	if h.aofChan == nil {
		return
	}
	h.aofChan <- &payload{
		cmdLine: cmdLine,
		dbIndex: dbIndex,
	}
}

func (h *AofHandler) handleAof() {
	for p := range h.aofChan {
		if p.dbIndex != h.currentDB {
			h.currentDB = p.dbIndex
			data := reply.GetMultiBulkReply(utils.String2Cmdline("SELECT", strconv.Itoa(h.currentDB))).ToBytes()
			_, err := h.aofFile.Write(data)
			if err != nil {
				logger.Error("AOF write error: " + err.Error())
				continue
			}
		}

		data := reply.GetMultiBulkReply(p.cmdLine).ToBytes()
		_, err := h.aofFile.Write(data)
		if err != nil {
			logger.Error("AOF write error: " + err.Error())
			continue
		}
	}
}

func (h *AofHandler) loadAof() {
	// Check if AOF file exists first
	if _, err := os.Stat(h.aofFilename); os.IsNotExist(err) {
		// File doesn't exist, this is normal for first startup
		logger.Info("AOF file not exists, skip loading")
		return
	}

	// Open the AOF file for reading
	aofFile, err := os.Open(h.aofFilename)
	if err != nil {
		logger.Error("AOF file open error: " + err.Error())
		return
	}
	defer func() {
		err := aofFile.Close()
		if err != nil {
			logger.Error("AOF file close error: " + err.Error())
		}
	}()

	ch := parser.ParseStream(aofFile)
	fakeConn := &connection.Connection{}
	fakeConn.SelectDB(0)

	for p := range ch {
		if p.Err != nil {
			// If the error is EOF or unexpected EOF, break the loop
			if p.Err == io.EOF || p.Err == io.ErrUnexpectedEOF {
				// End of file
				break
			}
			// Other errors
			logger.Error("AOF file parse error: " + p.Err.Error())
			continue
		}
		if p.Data == nil {
			logger.Error("AOF file empty payload")
			continue
		}
		// Attempt to parse the payload as a MultiBulkReply
		// If it fails, log an error and continue to the next payload
		r, ok := p.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("AOF file require multi bulk reply")
			continue
		}

		// 处理 SELECT 命令
		cmdName := strings.ToLower(string(r.Args[0]))
		if cmdName == "select" {
			if len(r.Args) != 2 {
				logger.Error("Invalid SELECT command in AOF file")
				continue
			}
			dbIndex, err := strconv.Atoi(string(r.Args[1]))
			if err != nil {
				logger.Errorf("Invalid DB index in SELECT command: %s", r.Args[1])
				continue
			}
			fakeConn.SelectDB(dbIndex)
			continue
		}

		// Execute the command on the database
		rep := h.db.Exec(fakeConn, r.Args)
		if rep == nil {
			// 将命令转换为字符串显示
			cmdStr := ""
			for i, arg := range r.Args {
				if i > 0 {
					cmdStr += " "
				}
				cmdStr += string(arg)
			}
			logger.Errorf("Execute AOF command returned nil: cmd=[%s]", cmdStr)
			continue
		}
		if reply.IsErrReply(rep) {
			// 将命令转换为字符串显示
			cmdStr := ""
			for i, arg := range r.Args {
				if i > 0 {
					cmdStr += " "
				}
				cmdStr += string(arg)
			}
			// 获取错误信息
			errMsg := ""
			if errRep, ok := rep.(*reply.StandardErrorReply); ok {
				errMsg = errRep.Status
			} else {
				errMsg = string(rep.ToBytes())
			}
			logger.Errorf("Execute AOF command error: cmd=[%s], error=[%s]", cmdStr, errMsg)
		}
	}
}
