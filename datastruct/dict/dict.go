package dict

type Consumer func(key string, value interface{}) bool

type Dict interface {
	Get(key string) (val interface{}, exists bool)
	Put(key string, value interface{}) (result int)         // put key-value pair, if exists, modify the value, return 0, if doesn't exist, add it, return 1
	PutIfAbsent(key string, value interface{}) (result int) // 如果key不存在则插入
	PutIfExists(key string, value interface{}) (result int)
	Len() int
	Remove(key string) (result int) // remove key-value pair, return the count of pairs
	ForEach(consumer Consumer)
	Keys() []string
	RandomKeys(n int) []string
	RandomDistinctKeys(n int) []string
	Clear() // clear all key-value pairs
}
