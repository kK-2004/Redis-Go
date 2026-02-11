package skiplist

import "math/rand"

const maxLevel = 16

// Level 表示跳表中节点的某一层
type Level struct {
	Forward *Node // 指向该层的下一个节点
	Span    int   // 跨度：从当前节点到 Forward 跨越的底层节点数
}

type Node struct {
	Member string
	Score  float64
	Levels []Level // 节点在所有层的指针和跨度
}

type SkipList struct {
	head   *Node
	tail   *Node
	level  int
	length int
	rand   *rand.Rand
	dict   map[string]*Node // member -> Node，O(1) 查找
}

func NewSkipList() *SkipList {
	head := &Node{
		Levels: make([]Level, maxLevel),
	}
	return &SkipList{
		head:   head,
		tail:   nil,
		level:  1,
		length: 0,
		rand:   rand.New(rand.NewSource(0)),
		dict:   make(map[string]*Node),
	}
}

func (sl *SkipList) randomLevel() int {
	level := 1
	for level < maxLevel && sl.rand.Float32() < 0.25 {
		level++
	}
	return level
}

// Insert 插入或更新节点
// 返回值: (是否是新插入, 分数是否发生变化)
func (sl *SkipList) Insert(member string, score float64) (isNew bool, scoreChanged bool) {
	// O(1) 查找是否已存在该 member
	existingNode, exists := sl.dict[member]
	if exists {
		if existingNode.Score == score {
			// member 已存在且 score 相同，不做任何处理
			return false, false
		}
		// member 已存在但 score 不同，需要更新
		// 先删除旧节点，再插入新节点
		sl.Delete(member, existingNode.Score)
		sl.insertNode(member, score)
		return true, true // score 发生变化
	}

	// member 不存在，执行插入
	sl.insertNode(member, score)
	return true, true
}

// insertNode 实际执行插入操作
func (sl *SkipList) insertNode(member string, score float64) {
	update := make([]*Node, maxLevel)
	rank := make([]int, maxLevel)

	x := sl.head

	// 从高层向下查找插入位置
	for i := sl.level - 1; i >= 0; i-- {
		if i == sl.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}

		for x.Levels[i].Forward != nil &&
			(x.Levels[i].Forward.Score < score ||
				(x.Levels[i].Forward.Score == score && x.Levels[i].Forward.Member < member)) {
			rank[i] += x.Levels[i].Span
			x = x.Levels[i].Forward
		}
		update[i] = x
	}

	level := sl.randomLevel()

	if level > sl.level {
		for i := sl.level; i < level; i++ {
			rank[i] = 0
			update[i] = sl.head
			// 新层的 span 指向即将插入的节点（span=1）含义：如果这里插入一个节点，从 head 走到它，需要 1 步。
			sl.head.Levels[i].Span = sl.length + 1
		}
		sl.level = level
	}

	newNode := &Node{
		Member: member,
		Score:  score,
		Levels: make([]Level, level),
	}

	for i := 0; i < level; i++ {
		newNode.Levels[i].Forward = update[i].Levels[i].Forward

		// Redis 核心公式
		newNode.Levels[i].Span = update[i].Levels[i].Span - (rank[0] - rank[i])
		update[i].Levels[i].Forward = newNode
		update[i].Levels[i].Span = (rank[0] - rank[i]) + 1
	}

	for i := level; i < sl.level; i++ {
		update[i].Levels[i].Span++
	}

	if newNode.Levels[0].Forward == nil {
		sl.tail = newNode
	}

	sl.length++
	sl.dict[member] = newNode // 添加到哈希表
}

// Delete removes an element from the skip list
func (sl *SkipList) Delete(member string, score float64) bool {
	// 先从哈希表检查是否存在
	existingNode, exists := sl.dict[member]
	if !exists || existingNode.Score != score {
		return false
	}

	update := make([]*Node, maxLevel)
	x := sl.head

	// 从高层向下查找目标节点
	for i := sl.level - 1; i >= 0; i-- {
		for x.Levels[i].Forward != nil &&
			(x.Levels[i].Forward.Score < score ||
				(x.Levels[i].Forward.Score == score && x.Levels[i].Forward.Member < member)) {
			x = x.Levels[i].Forward
		}
		update[i] = x
	}

	targetNode := update[0].Levels[0].Forward

	// 检查节点是否存在且匹配
	if targetNode != nil && targetNode.Score == score && targetNode.Member == member {
		// 在所有层级中移除目标节点，并更新 span
		for i := 0; i < sl.level; i++ {
			if update[i].Levels[i].Forward != targetNode {
				break // 目标节点不在这一层
			}
			// 前驱节点的 span += 目标节点的 span
			update[i].Levels[i].Span += targetNode.Levels[i].Span
			// 前驱节点的 forward 指向目标节点的下一个节点
			update[i].Levels[i].Forward = targetNode.Levels[i].Forward
		}

		// 更新尾节点指针
		if targetNode == sl.tail {
			if update[0] == sl.head {
				sl.tail = nil
			} else {
				sl.tail = update[0]
			}
		}

		// 更新 SkipList 的最大层级
		for sl.level > 1 && sl.head.Levels[sl.level-1].Forward == nil {
			sl.level--
		}

		sl.length--
		delete(sl.dict, member) // 从哈希表删除
		return true
	}

	return false
}

func (sl *SkipList) CountInRange(min float64, max float64) int {
	x := sl.head
	count := 0
	// 利用高层节点快速跳过不相关节点，找到第一个比min大的节点
	for i := sl.level - 1; i >= 0; i-- {
		for x.Levels[i].Forward != nil && x.Levels[i].Forward.Score < min {
			x = x.Levels[i].Forward
		}
	}
	x = x.Levels[0].Forward
	for x != nil && x.Score <= max {
		count++
		x = x.Levels[0].Forward
	}

	return count
}

func (sl *SkipList) RangeByScore(min, max float64, offset, count int) []string {
	ans := []string{}
	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for x.Levels[i].Forward != nil && x.Levels[i].Forward.Score < min {
			x = x.Levels[i].Forward
		}
	}

	x = x.Levels[0].Forward
	skipped := 0
	for x != nil && x.Score <= max {
		if offset < 0 || skipped >= offset {
			ans = append(ans, x.Member)
			if count > 0 && len(ans) >= count {
				break
			}
		} else {
			skipped++
		}
		x = x.Levels[0].Forward
	}
	return ans
}

// RangeByRank returns members by rank (position), 0-based index
func (sl *SkipList) RangeByRank(start, stop int) []string {
	result := []string{}

	// Handle negative indices
	if start < 0 {
		start = sl.length + start
	}
	if stop < 0 {
		stop = sl.length + stop
	}
	// Handle out of range
	if start < 0 {
		start = 0
	}
	if stop >= sl.length {
		stop = sl.length - 1
	}
	if start > stop || start >= sl.length || sl.length == 0 {
		return result
	}

	// Traverse to start position (0-based)
	x := sl.head.Levels[0].Forward
	for i := 0; i < start && x != nil; i++ {
		x = x.Levels[0].Forward
	}

	// Collect members from start to stop (inclusive)
	for i := start; i <= stop && x != nil; i++ {
		result = append(result, x.Member)
		x = x.Levels[0].Forward
	}

	return result
}

// GetRank returns the rank of the member with the given score (1-based)
// Time complexity: O(log n) using span information
func (sl *SkipList) GetRank(member string, score float64) int {
	rank := 0
	x := sl.head

	// 从高层向下查找，累加经过的 span
	for i := sl.level - 1; i >= 0; i-- {
		// 使用 <= 让 x 移动到最后一个 <= 目标的节点
		for x.Levels[i].Forward != nil &&
			(x.Levels[i].Forward.Score < score ||
				(x.Levels[i].Forward.Score == score && x.Levels[i].Forward.Member <= member)) {
			rank += x.Levels[i].Span
			x = x.Levels[i].Forward
		}
	}

	// 检查 x 是否是目标节点
	if x != sl.head && x.Score == score && x.Member == member {
		return rank
	}

	return -1 // 未找到
}
