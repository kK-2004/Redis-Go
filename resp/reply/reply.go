package reply

import (
	"Redis_Go/interface/resp"
	"bytes"
	"fmt"
	"strconv"
)

const (
	CRLF = "\r\n"
)

type ErrorReply interface {
	Error() string
	ToBytes() []byte
}

type BulkReply struct {
	Arg []byte
}

func (r *BulkReply) ToBytes() []byte {
	if r.Arg == nil {
		return GetNullBulkReply().ToBytes()
	}
	return []byte("$" + strconv.Itoa(len(r.Arg)) + CRLF + string(r.Arg) + CRLF)
}

func GetBulkReply(arg []byte) *BulkReply {
	return &BulkReply{
		Arg: arg,
	}
}

type MultiBulkReply struct {
	Args [][]byte
}

func (r *MultiBulkReply) ToBytes() []byte {
	argLen := len(r.Args)
	var buf bytes.Buffer
	buf.WriteString("*" + strconv.Itoa(argLen) + CRLF)
	for _, arg := range r.Args {
		if arg == nil {
			buf.WriteString(string(GetNullBulkReply().ToBytes()))
		} else {
			buf.WriteString(string(GetBulkReply(arg).ToBytes()))
		}
	}
	return buf.Bytes()
}

func GetMultiBulkReply(args [][]byte) *MultiBulkReply {
	return &MultiBulkReply{
		Args: args,
	}
}

// StandardErrorReply 状态回复(通用错误回复)
type StandardErrorReply struct {
	Status string
}

func (r *StandardErrorReply) ToBytes() []byte {
	return []byte("-" + r.Status + CRLF)
}

func GetStandardErrorReply(status string) *StandardErrorReply {
	return &StandardErrorReply{Status: status}
}

// IntReply 整数回复
type IntReply struct {
	Code int64
}

func (r *IntReply) ToBytes() []byte {
	return []byte(":" + strconv.FormatInt(r.Code, 10) + CRLF)
}

func GetIntReply(code int64) *IntReply {
	return &IntReply{
		Code: code,
	}
}

// StatusReply 状态回复
type StatusReply struct {
	Status string
}

func (r *StatusReply) ToBytes() []byte {
	return []byte("+" + r.Status + CRLF)
}

func GetStatusReply(status string) *StatusReply {
	return &StatusReply{
		Status: status,
	}
}

func IsErrReply(reply resp.Reply) bool {
	return reply.ToBytes()[0] == '-'
}

// MovedReply 表示键已移动到另一个节点的重定向响应
// 格式: -MOVED slot targetAddress\r\n
type MovedReply struct {
	Slot int
	Addr string
}

func (r *MovedReply) ToBytes() []byte {
	return []byte(fmt.Sprintf("-MOVED %d %s\r\n", r.Slot, r.Addr))
}

func (r *MovedReply) Error() string {
	return fmt.Sprintf("MOVED %d %s", r.Slot, r.Addr)
}

func MakeMovedReply(slot int, addr string) *MovedReply {
	return &MovedReply{Slot: slot, Addr: addr}
}
