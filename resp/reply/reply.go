package reply

import (
	"Redis_Go/interface/resp"
	"bytes"
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
	buf := make([]byte, 0, len(r.Arg)+16)
	buf = append(buf, '$')
	buf = strconv.AppendInt(buf, int64(len(r.Arg)), 10)
	buf = append(buf, '\r', '\n')
	buf = append(buf, r.Arg...)
	buf = append(buf, '\r', '\n')
	return buf
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
	var buf bytes.Buffer
	buf.Grow(16 + len(r.Args)*16)
	buf.WriteByte('*')
	buf.WriteString(strconv.Itoa(len(r.Args)))
	buf.WriteString(CRLF)
	for _, arg := range r.Args {
		if arg == nil {
			buf.WriteString("$-1")
			buf.WriteString(CRLF)
		} else {
			buf.WriteByte('$')
			buf.WriteString(strconv.Itoa(len(arg)))
			buf.WriteString(CRLF)
			buf.Write(arg)
			buf.WriteString(CRLF)
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
	buf := make([]byte, 0, 24)
	buf = append(buf, ':')
	buf = strconv.AppendInt(buf, r.Code, 10)
	buf = append(buf, '\r', '\n')
	return buf
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
	switch reply.(type) {
	case *StandardErrorReply, *ArgNumErrReply, *UnknownReply, *SyntaxErrReply, *WrongTypeErrReply, *ProtocolErrReply, *MovedReply:
		return true
	default:
		raw := reply.ToBytes()
		return len(raw) > 0 && raw[0] == '-'
	}
}

// MovedReply 表示键已移动到另一个节点的重定向响应
// 格式: -MOVED slot targetAddress\r\n
type MovedReply struct {
	Slot int
	Addr string
}

func (r *MovedReply) ToBytes() []byte {
	buf := make([]byte, 0, len(r.Addr)+24)
	buf = append(buf, '-', 'M', 'O', 'V', 'E', 'D', ' ')
	buf = strconv.AppendInt(buf, int64(r.Slot), 10)
	buf = append(buf, ' ')
	buf = append(buf, r.Addr...)
	buf = append(buf, '\r', '\n')
	return buf
}

func (r *MovedReply) Error() string {
	return "MOVED " + strconv.Itoa(r.Slot) + " " + r.Addr
}

func MakeMovedReply(slot int, addr string) *MovedReply {
	return &MovedReply{Slot: slot, Addr: addr}
}
