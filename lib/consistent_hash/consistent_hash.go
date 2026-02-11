package consistent_hash

import (
	"Redis_Go/lib/logger"
	"hash/crc32"
	"sort"
)

type NodeMap struct {
	hashFunc    func(data []byte) uint32
	nodeHashs   []int
	nodeHashMap map[int]string
}

func NewNodeMap(hashFunc func(data []byte) uint32) *NodeMap {
	m := &NodeMap{
		hashFunc:    hashFunc,
		nodeHashMap: make(map[int]string),
	}
	if m.hashFunc == nil {
		m.hashFunc = crc32.ChecksumIEEE
	}
	return m
}

func (m *NodeMap) IsEmpty() bool {
	return len(m.nodeHashMap) == 0
}

func (m *NodeMap) AddNode(nodes ...string) {
	for _, node := range nodes {
		if node == " " {
			continue
		}
		hash := int(m.hashFunc([]byte(node)))
		m.nodeHashs = append(m.nodeHashs, hash)
		m.nodeHashMap[hash] = node
	}
	sort.Ints(m.nodeHashs)
}

func (m *NodeMap) PickNode(key string) string {
	if m.IsEmpty() {
		return ""
	}
	hash := int(m.hashFunc([]byte(key)))
	tarHash := binarySearch(m.nodeHashs, hash)
	logger.Infof("PickNode: key=%s, keyHash=%d, nodeHashs=%v, tarHash=%d, selectedNode=%s",
		key, hash, m.nodeHashs, tarHash, m.nodeHashMap[m.nodeHashs[tarHash]])
	// 如果给定键值大于所有节点hash值，则返回第一个节点
	if tarHash == len(m.nodeHashs)-1 {
		tarHash = 0
	}
	return m.nodeHashMap[m.nodeHashs[tarHash]]
}

/*
 * 二分查找，返回大于等于目标值的第一个索引
 */
func binarySearch(arr []int, target int) int {
	l := 0
	r := len(arr) - 1
	for l < r {
		mid := (l + r) >> 1
		if arr[mid] < target {
			l = mid + 1
		} else {
			r = mid
		}
	}
	return l
}
