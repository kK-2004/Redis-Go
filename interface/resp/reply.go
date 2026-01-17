package resp

type Reply interface {
	ToBytes() []byte // 返回字节数组
}
