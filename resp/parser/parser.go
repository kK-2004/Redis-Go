package parser

import (
	"Redis_Go/interface/resp"
	"Redis_Go/lib/logger"
	"Redis_Go/resp/reply"
	"bufio"
	"errors"
	"io"
	"runtime/debug"
	"strconv"
	"strings"
)

type Payload struct {
	Data resp.Reply
	Err  error
}

type readState struct {
	readingMultiLine bool
	expectedArgsCnt  int
	msgType          byte
	args             [][]byte
	bulkLen          int64
}

func (r *readState) isDone() bool {
	return r.expectedArgsCnt > 0 && len(r.args) == r.expectedArgsCnt
}

func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parseIt(reader, ch)
	return ch
}

func parseIt(reader io.Reader, ch chan<- *Payload) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(string(debug.Stack()))
		}
	}()

	bufReader := bufio.NewReader(reader)
	var state readState
	var err error
	var msg []byte

	for {
		var ioErr bool
		msg, ioErr, err = readLine(bufReader, &state)

		if err != nil {
			if ioErr {
				ch <- &Payload{Err: err}
				close(ch)
				return
			}
			ch <- &Payload{Err: err}
			state = readState{}
			continue
		}

		// 非多行读取状态
		if !state.readingMultiLine {
			// 多条批量回复
			if msg[0] == '*' {
				// 解析头部，获取期望的参数数量
				err = parseMultiBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{Err: errors.New("Protocol error" + string(msg))}
					state = readState{} // 重置状态
					continue            // 继续循环，读取下一行
				}
				// 需要的参数数量为 0，直接返回
				if state.expectedArgsCnt == 0 {
					ch <- &Payload{Data: &reply.EmptyMultiBulkReply{}}
					state = readState{} // 重置状态
					continue            // 继续循环，读取下一行
				}
			} else if msg[0] == '$' {
				// Bulk 回复
				err = parseBulkHeader(msg, &state) // 解析 Bulk 回复的头部，获取 Bulk 回复的长度
				if err != nil {
					ch <- &Payload{Err: errors.New("Protocol error" + string(msg))}
					state = readState{} // 重置状态
					continue            // 继续循环，读取下一行
				}
				if state.bulkLen == -1 {
					// Bulk 回复的长度为 0，直接返回
					ch <- &Payload{Data: &reply.NullBulkReply{}}
					state = readState{} // 重置状态
					continue            // 继续循环，读取下一行
				}
			} else {
				// 单行回复
				result, err := parseSingleLineReply(msg)
				ch <- &Payload{Data: result, Err: err}
				state = readState{} // 本条消息已结束，重置状态
				continue            // 继续循环，读取下一行
			}
		} else {
			err = readBody(msg, &state)
			if err != nil {
				ch <- &Payload{
					Err: errors.New("protocol error: " + string(msg)),
				}
				state = readState{} // reset state
				continue
			}
			// 如果解析完成，返回结果
			if state.isDone() {
				var result resp.Reply
				if state.msgType == '*' {
					result = reply.GetMultiBulkReply(state.args)
				} else if state.msgType == '$' {
					result = reply.GetBulkReply(state.args[0])
				}
				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				state = readState{}
			}
		}
	}
}

// 符合RESP协议时返回false
func readLine(bufReader *bufio.Reader, state *readState) ([]byte, bool, error) {
	var line []byte
	var err error
	if state.bulkLen == 0 {
		line, err = bufReader.ReadBytes('\n')
		if err != nil {
			return nil, false, err
		}
		if len(line) == 0 || line[len(line)-2] != '\r' {
			return nil, false, errors.New("protocol error: bad bulk length")
		}
	} else {
		line = make([]byte, state.bulkLen+2)
		_, err = io.ReadFull(bufReader, line)
		if err != nil {
			return nil, false, err
		}
		if len(line) == 0 || line[len(line)-2] != '\r' || line[len(line)-1] != '\n' {
			// 不符合RESP协议
			return nil, false, errors.New("protocol error: bad bulk string")
		}
		state.bulkLen = 0
	}
	return line, false, nil
}

func parseMultiBulkHeader(msg []byte, state *readState) error {
	var err error
	var expectedLine uint64
	expectedLine, err = strconv.ParseUint(string(msg[1:len(msg)-2]), 10, 32)
	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}
	if expectedLine == 0 {
		state.expectedArgsCnt = 0
		return nil
	} else if expectedLine > 0 {
		// 多行读取的
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.expectedArgsCnt = int(expectedLine)
		state.args = make([][]byte, 0, expectedLine)
		return nil
	} else {
		return errors.New("protocol error: " + string(msg))
	}
}

func parseBulkHeader(msg []byte, state *readState) error {
	var err error
	state.bulkLen, err = strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}
	if state.bulkLen == -1 { // null bulk
		return nil
	} else if state.bulkLen > 0 {
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.expectedArgsCnt = 1
		state.args = make([][]byte, 0, 1)
		return nil
	} else {
		return errors.New("protocol error: " + string(msg))
	}
}

func parseSingleLineReply(msg []byte) (resp.Reply, error) {
	str := strings.TrimSuffix(string(msg), "\r\n")
	var result resp.Reply
	switch msg[0] {
	case '+': // status reply
		result = reply.GetStatusReply(str[1:])
	case '-': // err reply
		result = reply.GetStandardErrorReply(str[1:])
	case ':': // int reply
		val, err := strconv.ParseInt(str[1:], 10, 64)
		if err != nil {
			return nil, errors.New("protocol error: " + string(msg))
		}
		result = reply.GetIntReply(val)
	}
	return result, nil
}

func readBody(msg []byte, state *readState) error {
	line := msg[0 : len(msg)-2]
	var err error
	if line[0] == '$' {
		state.bulkLen, err = strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return errors.New("protocol error: " + string(msg))
		}
		if state.bulkLen <= 0 {
			state.args = append(state.args, nil)
			state.bulkLen = 0
		}
	} else {
		state.args = append(state.args, line)
	}
	return nil
}
