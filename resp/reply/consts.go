package reply

type PongReply struct {
}

func (r *PongReply) ToBytes() []byte {
	return []byte("+PONG\r\n")
}
func GetPongReply() *PongReply {
	return &PongReply{}
}

type OKReply struct {
}

func (r *OKReply) ToBytes() []byte {
	return []byte("+OK\r\n")
}
func GetOKReply() *OKReply {
	return &OKReply{}
}

type NullBulkReply struct {
}

func (r *NullBulkReply) ToBytes() []byte {
	return []byte("$-1\r\n")
}
func GetNullBulkReply() *NullBulkReply {
	return &NullBulkReply{}
}

type EmptyMultiBulkReply struct {
}

func (r *EmptyMultiBulkReply) ToBytes() []byte {
	return []byte("$0\r\n\r\n")
}
func GetEmptyMultiBulkReply() *EmptyMultiBulkReply {
	return &EmptyMultiBulkReply{}
}

type NoReply struct {
}

func (r *NoReply) ToBytes() []byte {
	return []byte("")
}
func MakeNoReply() *NoReply {
	return &NoReply{}
}
