package consistent_hash

import (
	"fmt"
	"hash/crc32"
	"sort"
	"strings"
)

const defaultReplicas = 100

type ringEntry struct {
	hash int
	node string
}

type NodeMap struct {
	hashFunc func(data []byte) uint32
	replicas int
	ring     []ringEntry
}

func NewNodeMap(replicas int, hashFunc func(data []byte) uint32) *NodeMap {
	if replicas <= 0 {
		replicas = defaultReplicas
	}
	m := &NodeMap{
		hashFunc: hashFunc,
		replicas: replicas,
	}
	if m.hashFunc == nil {
		m.hashFunc = crc32.ChecksumIEEE
	}
	return m
}

func (m *NodeMap) IsEmpty() bool {
	return len(m.ring) == 0
}

func (m *NodeMap) AddNode(nodes ...string) {
	for _, node := range nodes {
		node = strings.TrimSpace(node)
		if node == "" {
			continue
		}
		for i := 0; i < m.replicas; i++ {
			virtualNode := fmt.Sprintf("%s#%d", node, i)
			hash := int(m.hashFunc([]byte(virtualNode)))
			m.ring = append(m.ring, ringEntry{
				hash: hash,
				node: node,
			})
		}
	}
	sort.Slice(m.ring, func(i, j int) bool {
		if m.ring[i].hash == m.ring[j].hash {
			return m.ring[i].node < m.ring[j].node
		}
		return m.ring[i].hash < m.ring[j].hash
	})
}

func (m *NodeMap) PickNode(key string) string {
	if m.IsEmpty() {
		return ""
	}
	hash := int(m.hashFunc([]byte(key)))
	idx := sort.Search(len(m.ring), func(i int) bool {
		return m.ring[i].hash >= hash
	})
	if idx == len(m.ring) {
		idx = 0
	}
	return m.ring[idx].node
}
