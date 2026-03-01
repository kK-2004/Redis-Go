package set

import (
	"math/rand"
)

const (
	encodingListpack = 0
	encodingDict     = 1

	// 编码转换阈值
	setMaxListpackEntries = 128 // 元素数量超过此值转换为哈希表
)

// Set 集合数据结构
// 使用 listpack（slice）+ 哈希表（map）的混合编码
type Set struct {
	encoding  int              // 当前编码
	listpack  []string         // listpack 编码：紧凑存储
	dict      map[string]struct{} // 哈希表编码：O(1) 查找
}

// NewSet 创建新的 Set
func NewSet() *Set {
	return &Set{
		encoding: encodingListpack,
		listpack: make([]string, 0),
	}
}

// Add 添加元素，返回是否新增
func (s *Set) Add(member string) bool {
	// 检查是否需要转换编码
	if s.encoding == encodingListpack {
		// 先检查是否已存在
		for _, m := range s.listpack {
			if m == member {
				return false
			}
		}
		// 检查是否需要转换
		if len(s.listpack) >= setMaxListpackEntries {
			s.convertToDict()
			s.dict[member] = struct{}{}
			return true
		}
		s.listpack = append(s.listpack, member)
		return true
	}

	// 哈希表编码
	if _, exists := s.dict[member]; exists {
		return false
	}
	s.dict[member] = struct{}{}
	return true
}

// Remove 删除元素，返回是否删除成功
func (s *Set) Remove(member string) bool {
	if s.encoding == encodingListpack {
		for i, m := range s.listpack {
			if m == member {
				s.listpack = append(s.listpack[:i], s.listpack[i+1:]...)
				return true
			}
		}
		return false
	}

	if _, exists := s.dict[member]; !exists {
		return false
	}
	delete(s.dict, member)
	return true
}

// Contains 检查元素是否存在
func (s *Set) Contains(member string) bool {
	if s.encoding == encodingListpack {
		for _, m := range s.listpack {
			if m == member {
				return true
			}
		}
		return false
	}

	_, exists := s.dict[member]
	return exists
}

// Len 返回元素数量
func (s *Set) Len() int {
	if s.encoding == encodingListpack {
		return len(s.listpack)
	}
	return len(s.dict)
}

// Members 返回所有元素
func (s *Set) Members() []string {
	if s.encoding == encodingListpack {
		result := make([]string, len(s.listpack))
		copy(result, s.listpack)
		return result
	}

	result := make([]string, 0, len(s.dict))
	for member := range s.dict {
		result = append(result, member)
	}
	return result
}

// RandomMember 随机返回一个元素
func (s *Set) RandomMember() (string, bool) {
	if s.Len() == 0 {
		return "", false
	}

	if s.encoding == encodingListpack {
		idx := rand.Intn(len(s.listpack))
		return s.listpack[idx], true
	}

	// 从 map 中随机取一个
	idx := rand.Intn(len(s.dict))
	for member := range s.dict {
		if idx == 0 {
			return member, true
		}
		idx--
	}
	return "", false
}

// RandomMembers 随机返回多个元素（可能重复）
func (s *Set) RandomMembers(count int) []string {
	if s.Len() == 0 {
		return []string{}
	}

	result := make([]string, 0, count)
	if s.encoding == encodingListpack {
		for i := 0; i < count; i++ {
			idx := rand.Intn(len(s.listpack))
			result = append(result, s.listpack[idx])
		}
		return result
	}

	// 哈希表编码
	members := s.Members()
	for i := 0; i < count; i++ {
		idx := rand.Intn(len(members))
		result = append(result, members[idx])
	}
	return result
}

// RandomDistinctMembers 随机返回多个不重复的元素
func (s *Set) RandomDistinctMembers(count int) []string {
	size := s.Len()
	if size == 0 {
		return []string{}
	}

	// 如果请求数量大于等于集合大小，返回所有元素
	if count >= size {
		return s.Members()
	}

	result := make([]string, 0, count)
	if s.encoding == encodingListpack {
		// 使用 Fisher-Yates 洗牌算法
		shuffled := make([]string, size)
		copy(shuffled, s.listpack)
		for i := 0; i < count; i++ {
			j := i + rand.Intn(size-i)
			shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
			result = append(result, shuffled[i])
		}
		return result
	}

	// 哈希表编码
	members := s.Members()
	for i := 0; i < count; i++ {
		j := i + rand.Intn(size-i)
		members[i], members[j] = members[j], members[i]
		result = append(result, members[i])
	}
	return result
}

// Pop 随机弹出元素
func (s *Set) Pop(count int) []string {
	size := s.Len()
	if size == 0 {
		return []string{}
	}

	// 如果请求数量大于等于集合大小，弹出所有元素
	if count >= size {
		result := s.Members()
		if s.encoding == encodingListpack {
			s.listpack = s.listpack[:0]
		} else {
			s.dict = make(map[string]struct{})
		}
		return result
	}

	result := make([]string, 0, count)
	if s.encoding == encodingListpack {
		for i := 0; i < count; i++ {
			idx := rand.Intn(len(s.listpack))
			result = append(result, s.listpack[idx])
			s.listpack = append(s.listpack[:idx], s.listpack[idx+1:]...)
		}
		return result
	}

	// 哈希表编码
	for i := 0; i < count; i++ {
		for member := range s.dict {
			result = append(result, member)
			delete(s.dict, member)
			break
		}
	}
	return result
}

// Union 返回与其他集合的并集
func (s *Set) Union(other *Set) *Set {
	result := NewSet()

	// 添加当前集合的所有元素
	if s.encoding == encodingListpack {
		for _, m := range s.listpack {
			result.Add(m)
		}
	} else {
		for m := range s.dict {
			result.Add(m)
		}
	}

	// 添加其他集合的所有元素
	if other.encoding == encodingListpack {
		for _, m := range other.listpack {
			result.Add(m)
		}
	} else {
		for m := range other.dict {
			result.Add(m)
		}
	}

	return result
}

// Intersect 返回与其他集合的交集
func (s *Set) Intersect(other *Set) *Set {
	// 优化：遍历较小的集合
	if s.Len() > other.Len() {
		return other.Intersect(s)
	}

	result := NewSet()
	if s.encoding == encodingListpack {
		for _, m := range s.listpack {
			if other.Contains(m) {
				result.Add(m)
			}
		}
	} else {
		for m := range s.dict {
			if other.Contains(m) {
				result.Add(m)
			}
		}
	}

	return result
}

// Diff 返回与其他集合的差集（在 s 中但不在 other 中）
func (s *Set) Diff(other *Set) *Set {
	result := NewSet()
	if s.encoding == encodingListpack {
		for _, m := range s.listpack {
			if !other.Contains(m) {
				result.Add(m)
			}
		}
	} else {
		for m := range s.dict {
			if !other.Contains(m) {
				result.Add(m)
			}
		}
	}

	return result
}

// Encoding 返回当前编码类型
func (s *Set) Encoding() int {
	return s.encoding
}

// Clear 清空集合
func (s *Set) Clear() {
	s.listpack = nil
	s.dict = nil
	s.encoding = encodingListpack
	s.listpack = make([]string, 0)
}

// convertToDict 转换为哈希表编码
func (s *Set) convertToDict() {
	s.dict = make(map[string]struct{}, len(s.listpack))
	for _, m := range s.listpack {
		s.dict[m] = struct{}{}
	}
	s.listpack = nil
	s.encoding = encodingDict
}
