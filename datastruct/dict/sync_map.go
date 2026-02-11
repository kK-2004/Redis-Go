package dict

import (
	"Redis_Go/lib/sync/wait"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type SyncDict struct {
	m       sync.Map
	wg      wait.Wait
	closing atomic.Bool
}

// 引入do模板，clear时等待所有操作结束 ⭐️
func (dict *SyncDict) do(action func()) bool {
	if dict.closing.Load() {
		return false
	}
	dict.wg.Add(1)
	defer dict.wg.Done()
	if dict.closing.Load() {
		return false
	}
	action()
	return true
}

func GetSyncDict() *SyncDict {
	return &SyncDict{}
}

func (dict *SyncDict) Get(key string) (val interface{}, exists bool) {
	ok := dict.do(func() {
		val, exists = dict.m.Load(key) // 利用闭包特性（: 而不是:=）
	})
	if !ok {
		return nil, false
	}
	return
}

func (dict *SyncDict) Put(key string, val interface{}) (result int) {
	ok := dict.do(func() {
		_, exists := dict.m.Swap(key, val)
		if exists {
			result = 0
		} else {
			result = 1
		}
	})

	if !ok {
		return 0
	}
	return
}

//	func (dict *SyncDict) PutIfExists(key string, val interface{}) (result int) {
//		//oldV, exists := dict.m.Load(key)
//		//if exists {
//		//	dict.m.Store(key, val)
//		//	logger.Info("key %s exists, modified oldV[%v] -> newV[%v]", key, oldV, val)
//		//	return 1
//		//}
//		//logger.Info("key %s not exists, put failed", key)
//		//return 0
//
//		// cas
//		for {
//			old, exists := dict.m.Load(key)
//			if !exists {
//				return 0 // key does not exist
//			}
//			// Atomically compare and swap the value
//			if dict.m.CompareAndSwap(key, old, val) {
//				return 1 // successfully updated
//			}
//			// CAS failed, another goroutine modified the value, retry
//		}
//	}
func (dict *SyncDict) PutIfExists(key string, val interface{}) (result int) {
	dict.do(func() {
		for {
			old, exists := dict.m.Load(key)
			if !exists {
				result = 0
				return
			}
			if dict.m.CompareAndSwap(key, old, val) {
				result = 1
				return
			}
		}
	})
	return result
}

//	func (dict *SyncDict) PutIfAbsent(key string, val interface{}) (result int) {
//		_, exists := dict.m.LoadOrStore(key, val)
//		if exists {
//			logger.Info("key %s exists, put failed", key)
//			return 0
//		}
//		dict.m.Store(key, val)
//		logger.Info("key %s put V[%v]", key, val)
//		return 1
//	}
func (dict *SyncDict) PutIfAbsent(key string, val interface{}) (result int) {
	dict.do(func() {
		_, exists := dict.m.LoadOrStore(key, val)
		if exists {
			result = 0
			return
		}
		dict.m.Store(key, val)
		result = 1
	})
	return result
}

//func (dict *SyncDict) Remove(key string) (result int) {
//	_, exists := dict.m.Load(key)
//	if exists {
//		dict.m.Delete(key)
//		logger.Info("key %s exists, removed", key)
//		return 1
//	}
//	logger.Info("key %s not exists, remove failed", key)
//	return 0
//}

func (dict *SyncDict) Remove(key string) (result int) {
	dict.do(func() {
		_, exists := dict.m.Load(key)
		if exists {
			dict.m.Delete(key)
			result = 1
			return
		}
		result = 0
	})
	return result
}

//func (dict *SyncDict) Len() int {
//	size := 0
//	dict.m.Range(func(key, value interface{}) bool {
//		size++
//		return true
//	})
//	return size
//}

func (dict *SyncDict) Len() int {
	size := 0
	dict.do(func() {
		dict.m.Range(func(key, value interface{}) bool {
			size++
			return true
		})
	})
	return size
}

//func (dict *SyncDict) ForEach(consumer Consumer) {
//	dict.m.Range(func(key, value interface{}) bool {
//		consumer(key.(string), value)
//		return true
//	})
//}

func (dict *SyncDict) ForEach(consumer Consumer) {
	_ = dict.do(func() {
		dict.m.Range(func(key, value interface{}) bool {
			consumer(key.(string), value)
			return true
		})
	})
}

//func (dict *SyncDict) Keys() []string {
//	keys := make([]string, 0, dict.Len())
//	dict.ForEach(func(key string, value interface{}) bool {
//		keys = append(keys, key)
//		return true
//	})
//	return keys
//}

func (dict *SyncDict) Keys() []string {
	keys := make([]string, 0, dict.Len())
	_ = dict.do(func() {
		dict.ForEach(func(key string, value interface{}) bool {
			keys = append(keys, key)
			return true
		})
	})
	return keys
}

//func (dict *SyncDict) RandomKeys(n int) []string {
//	keys := dict.Keys()
//	if len(keys) == 0 || n <= 0 {
//		return nil
//	}
//	res := make([]string, n)
//	for i := 0; i < n; i++ {
//		res[i] = keys[rand.Intn(len(keys))]
//	}
//	return res
//}

func (dict *SyncDict) RandomKeys(n int) []string {
	keys := dict.Keys()
	if len(keys) == 0 || n <= 0 {
		return nil
	}
	res := make([]string, n)
	for i := 0; i < n; i++ {
		res[i] = keys[rand.Intn(len(keys))]
	}
	return res
}

//func (dict *SyncDict) RandomDistinctKeys(n int) []string {
//	keys := dict.Keys()
//	for i := 0; i < len(keys); i++ {
//		j := i + rand.Intn(len(keys)-i)
//		keys[i], keys[j] = keys[j], keys[i]
//	}
//	return keys[:min(len(keys), n)]
//}

func (dict *SyncDict) RandomDistinctKeys(n int) []string {
	keys := dict.Keys()
	for i := 0; i < len(keys); i++ {
		j := i + rand.Intn(len(keys)-i)
		keys[i], keys[j] = keys[j], keys[i]
	}
	return keys[:min(len(keys), n)]
}

func (dict *SyncDict) Clear() {
	dict.closing.Store(true)
	_ = dict.wg.WaitWithTimeout(10 * time.Second)
	*dict = *GetSyncDict()
}
