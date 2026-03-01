package database

import (
	"Redis_Go/datastruct/set"
	"Redis_Go/interface/database"
	"Redis_Go/interface/resp"
	"Redis_Go/lib/utils"
	"Redis_Go/lib/wildcard"
	"Redis_Go/resp/reply"
	"strconv"
)

// SADD key member [member ...]
func execSAdd(db *DB, args [][]byte) resp.Reply {
	if len(args) < 2 {
		return reply.GetArgNumErrReply("SADD")
	}

	key := string(args[0])
	var (
		added    int
		errReply resp.Reply
	)

	db.WithKeyLock(key, func() {
		setObj, _ := db.getOrCreateSet(key)
		if setObj == nil {
			errReply = reply.GetWrongTypeErrReply()
			return
		}
		for i := 1; i < len(args); i++ {
			if setObj.Add(string(args[i])) {
				added++
			}
		}
		db.addAof(utils.ToCmdLineWithName("SADD", args...))
	})

	if errReply != nil {
		return errReply
	}
	return reply.GetIntReply(int64(added))
}

// SREM key member [member ...]
func execSRem(db *DB, args [][]byte) resp.Reply {
	if len(args) < 2 {
		return reply.GetArgNumErrReply("SREM")
	}

	key := string(args[0])
	var (
		removed  int
		errReply resp.Reply
	)

	db.WithKeyLock(key, func() {
		setObj, exists := db.getAsSet(key)
		if isWrongTypeSet(setObj, exists) {
			errReply = reply.GetWrongTypeErrReply()
			return
		}
		if !exists {
			return
		}
		for i := 1; i < len(args); i++ {
			if setObj.Remove(string(args[i])) {
				removed++
			}
		}
		if setObj.Len() == 0 {
			db.Remove(key)
		}
		db.addAof(utils.ToCmdLineWithName("SREM", args...))
	})

	if errReply != nil {
		return errReply
	}
	return reply.GetIntReply(int64(removed))
}

// SISMEMBER key member
func execSIsMember(db *DB, args [][]byte) resp.Reply {
	if len(args) != 2 {
		return reply.GetArgNumErrReply("SISMEMBER")
	}

	key := string(args[0])
	member := string(args[1])
	var (
		exists   bool
		errReply resp.Reply
	)

	db.WithRKeyLock(key, func() {
		setObj, ok := db.getAsSet(key)
		if isWrongTypeSet(setObj, ok) {
			errReply = reply.GetWrongTypeErrReply()
			return
		}
		if !ok {
			return
		}
		exists = setObj.Contains(member)
	})

	if errReply != nil {
		return errReply
	}
	if exists {
		return reply.GetIntReply(1)
	}
	return reply.GetIntReply(0)
}

// SMEMBERS key
func execSMembers(db *DB, args [][]byte) resp.Reply {
	if len(args) != 1 {
		return reply.GetArgNumErrReply("SMEMBERS")
	}

	key := string(args[0])
	var (
		members  []string
		errReply resp.Reply
	)

	db.WithRKeyLock(key, func() {
		setObj, exists := db.getAsSet(key)
		if isWrongTypeSet(setObj, exists) {
			errReply = reply.GetWrongTypeErrReply()
			return
		}
		if !exists {
			return
		}
		members = setObj.Members()
	})

	if errReply != nil {
		return errReply
	}
	result := make([][]byte, len(members))
	for i, m := range members {
		result[i] = []byte(m)
	}
	return reply.GetMultiBulkReply(result)
}

// SCARD key
func execSCard(db *DB, args [][]byte) resp.Reply {
	if len(args) != 1 {
		return reply.GetArgNumErrReply("SCARD")
	}

	key := string(args[0])
	var (
		count    int
		errReply resp.Reply
	)

	db.WithRKeyLock(key, func() {
		setObj, exists := db.getAsSet(key)
		if isWrongTypeSet(setObj, exists) {
			errReply = reply.GetWrongTypeErrReply()
			return
		}
		if !exists {
			return
		}
		count = setObj.Len()
	})

	if errReply != nil {
		return errReply
	}
	return reply.GetIntReply(int64(count))
}

// SPOP key [count]
func execSPop(db *DB, args [][]byte) resp.Reply {
	if len(args) < 1 || len(args) > 2 {
		return reply.GetArgNumErrReply("SPOP")
	}

	key := string(args[0])
	count := 1
	if len(args) == 2 {
		c, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil || c <= 0 {
			return reply.GetStandardErrorReply("ERR value is out of range, must be positive")
		}
		count = int(c)
	}

	var (
		members  []string
		errReply resp.Reply
	)

	db.WithKeyLock(key, func() {
		setObj, exists := db.getAsSet(key)
		if isWrongTypeSet(setObj, exists) {
			errReply = reply.GetWrongTypeErrReply()
			return
		}
		if !exists {
			return
		}
		members = setObj.Pop(count)
		if setObj.Len() == 0 {
			db.Remove(key)
		}
		db.addAof(utils.ToCmdLineWithName("SPOP", args...))
	})

	if errReply != nil {
		return errReply
	}
	if len(members) == 0 {
		return reply.GetNullBulkReply()
	}

	if len(args) == 1 {
		return reply.GetBulkReply([]byte(members[0]))
	}

	result := make([][]byte, len(members))
	for i, m := range members {
		result[i] = []byte(m)
	}
	return reply.GetMultiBulkReply(result)
}

// SRANDMEMBER key [count]
func execSRandMember(db *DB, args [][]byte) resp.Reply {
	if len(args) < 1 || len(args) > 2 {
		return reply.GetArgNumErrReply("SRANDMEMBER")
	}

	key := string(args[0])
	count := 1
	if len(args) == 2 {
		c, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err == nil {
			count = int(c)
		}
	}

	var (
		members  []string
		errReply resp.Reply
	)

	db.WithRKeyLock(key, func() {
		setObj, exists := db.getAsSet(key)
		if isWrongTypeSet(setObj, exists) {
			errReply = reply.GetWrongTypeErrReply()
			return
		}
		if !exists {
			return
		}

		if len(args) == 1 {
			if m, ok := setObj.RandomMember(); ok {
				members = []string{m}
			}
		} else if count > 0 {
			members = setObj.RandomDistinctMembers(count)
		} else {
			members = setObj.RandomMembers(-count)
		}
	})

	if errReply != nil {
		return errReply
	}
	if len(members) == 0 {
		if len(args) == 1 {
			return reply.GetNullBulkReply()
		}
		return reply.GetEmptyMultiBulkReply()
	}

	if len(args) == 1 {
		return reply.GetBulkReply([]byte(members[0]))
	}

	result := make([][]byte, len(members))
	for i, m := range members {
		result[i] = []byte(m)
	}
	return reply.GetMultiBulkReply(result)
}

// SMOVE source destination member
func execSMove(db *DB, args [][]byte) resp.Reply {
	if len(args) != 3 {
		return reply.GetArgNumErrReply("SMOVE")
	}

	srcKey := string(args[0])
	destKey := string(args[1])
	member := string(args[2])

	// Handle same-key case as a no-op (avoids deadlock from double lock on same key)
	if srcKey == destKey {
		lock := db.lockMgr.Lock(srcKey)
		defer db.lockMgr.Unlock(lock)

		srcSet, exists := db.getAsSet(srcKey)
		if isWrongTypeSet(srcSet, exists) {
			return reply.GetWrongTypeErrReply()
		}
		if !exists {
			return reply.GetIntReply(0)
		}

		if srcSet.Contains(member) {
			return reply.GetIntReply(1)
		}
		return reply.GetIntReply(0)
	}

	// Different keys - need to lock both to prevent deadlock
	keys := []string{srcKey, destKey}
	if srcKey > destKey {
		keys = []string{destKey, srcKey}
	}

	var moved bool

	lock1 := db.lockMgr.Lock(keys[0])
	defer db.lockMgr.Unlock(lock1)

	lock2 := db.lockMgr.Lock(keys[1])
	defer db.lockMgr.Unlock(lock2)

	srcSet, exists := db.getAsSet(srcKey)
	if isWrongTypeSet(srcSet, exists) {
		return reply.GetWrongTypeErrReply()
	}
	if !exists {
		return reply.GetIntReply(0)
	}

	if !srcSet.Contains(member) {
		return reply.GetIntReply(0)
	}

	destSet, _ := db.getOrCreateSet(destKey)
	if destSet == nil {
		return reply.GetWrongTypeErrReply()
	}

	srcSet.Remove(member)
	if srcSet.Len() == 0 {
		db.Remove(srcKey)
	}
	destSet.Add(member)

	moved = true
	db.addAof(utils.ToCmdLineWithName("SMOVE", args...))

	if moved {
		return reply.GetIntReply(1)
	}
	return reply.GetIntReply(0)
}

// SUNION key [key ...]
func execSUnion(db *DB, args [][]byte) resp.Reply {
	if len(args) < 1 {
		return reply.GetArgNumErrReply("SUNION")
	}

	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = string(arg)
	}

	sortedKeys := utils.DedupSortedKeys(keys)
	locks := make([]*KeyLockHandle, len(sortedKeys))
	for i, key := range sortedKeys {
		locks[i] = db.lockMgr.RLock(key)
	}
	defer func() {
		for _, lock := range locks {
			db.lockMgr.RUnlock(lock)
		}
	}()

	sets := make([]*set.Set, 0)
	for _, key := range keys {
		if setObj, exists := db.getAsSet(key); exists {
			if setObj == nil {
				return reply.GetWrongTypeErrReply()
			}
			sets = append(sets, setObj)
		}
	}

	if len(sets) == 0 {
		return reply.GetEmptyMultiBulkReply()
	}

	result := sets[0]
	for i := 1; i < len(sets); i++ {
		result = result.Union(sets[i])
	}

	members := result.Members()
	resultBytes := make([][]byte, len(members))
	for i, m := range members {
		resultBytes[i] = []byte(m)
	}
	return reply.GetMultiBulkReply(resultBytes)
}

// SINTER key [key ...]
func execSInter(db *DB, args [][]byte) resp.Reply {
	if len(args) < 1 {
		return reply.GetArgNumErrReply("SINTER")
	}

	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = string(arg)
	}

	sortedKeys := utils.DedupSortedKeys(keys)
	locks := make([]*KeyLockHandle, len(sortedKeys))
	for i, key := range sortedKeys {
		locks[i] = db.lockMgr.RLock(key)
	}
	defer func() {
		for _, lock := range locks {
			db.lockMgr.RUnlock(lock)
		}
	}()

	sets := make([]*set.Set, 0)
	for _, key := range keys {
		if setObj, exists := db.getAsSet(key); exists {
			if setObj == nil {
				return reply.GetWrongTypeErrReply()
			}
			sets = append(sets, setObj)
		} else {
			return reply.GetEmptyMultiBulkReply()
		}
	}

	if len(sets) == 0 {
		return reply.GetEmptyMultiBulkReply()
	}

	result := sets[0]
	for i := 1; i < len(sets); i++ {
		result = result.Intersect(sets[i])
	}

	members := result.Members()
	resultBytes := make([][]byte, len(members))
	for i, m := range members {
		resultBytes[i] = []byte(m)
	}
	return reply.GetMultiBulkReply(resultBytes)
}

// SDIFF key [key ...]
func execSDiff(db *DB, args [][]byte) resp.Reply {
	if len(args) < 1 {
		return reply.GetArgNumErrReply("SDIFF")
	}

	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = string(arg)
	}

	sortedKeys := utils.DedupSortedKeys(keys)
	locks := make([]*KeyLockHandle, len(sortedKeys))
	for i, key := range sortedKeys {
		locks[i] = db.lockMgr.RLock(key)
	}
	defer func() {
		for _, lock := range locks {
			db.lockMgr.RUnlock(lock)
		}
	}()

	// The first key is the base set for diff - if it doesn't exist, result is empty
	firstSet, exists := db.getAsSet(keys[0])
	if isWrongTypeSet(firstSet, exists) {
		return reply.GetWrongTypeErrReply()
	}
	if !exists {
		return reply.GetEmptyMultiBulkReply()
	}

	// Subsequent keys are subtracted from the first (missing keys just don't subtract)
	sets := make([]*set.Set, 1, len(keys))
	sets[0] = firstSet
	for i := 1; i < len(keys); i++ {
		if setObj, exists := db.getAsSet(keys[i]); exists {
			if setObj == nil {
				return reply.GetWrongTypeErrReply()
			}
			sets = append(sets, setObj)
		}
	}

	result := sets[0]
	for i := 1; i < len(sets); i++ {
		result = result.Diff(sets[i])
	}

	members := result.Members()
	resultBytes := make([][]byte, len(members))
	for i, m := range members {
		resultBytes[i] = []byte(m)
	}
	return reply.GetMultiBulkReply(resultBytes)
}

// SUNIONSTORE destination key [key ...]
func execSUnionStore(db *DB, args [][]byte) resp.Reply {
	if len(args) < 2 {
		return reply.GetArgNumErrReply("SUNIONSTORE")
	}

	destKey := string(args[0])
	srcKeys := make([]string, len(args)-1)
	for i, arg := range args[1:] {
		srcKeys[i] = string(arg)
	}

	allKeys := append([]string{destKey}, srcKeys...)
	// 去重避免重复锁定同一键造成死锁（例如 SUNIONSTORE k k）
	sortedKeys := utils.DedupSortedKeys(allKeys)
	locks := make([]*KeyLockHandle, len(sortedKeys))
	for i, key := range sortedKeys {
		locks[i] = db.lockMgr.Lock(key)
	}
	defer func() {
		for _, lock := range locks {
			db.lockMgr.Unlock(lock)
		}
	}()

	sets := make([]*set.Set, 0)
	for _, key := range srcKeys {
		if setObj, exists := db.getAsSet(key); exists {
			if setObj == nil {
				return reply.GetWrongTypeErrReply()
			}
			sets = append(sets, setObj)
		}
	}

	if len(sets) == 0 {
		db.Remove(destKey)
		db.addAof(utils.ToCmdLineWithName("SUNIONSTORE", args...))
		return reply.GetIntReply(0)
	}

	result := sets[0]
	for i := 1; i < len(sets); i++ {
		result = result.Union(sets[i])
	}

	destSet := set.NewSet()
	for _, m := range result.Members() {
		destSet.Add(m)
	}
	db.PutEntity(destKey, &database.DataEntity{Data: destSet})

	db.addAof(utils.ToCmdLineWithName("SUNIONSTORE", args...))
	return reply.GetIntReply(int64(destSet.Len()))
}

// SINTERSTORE destination key [key ...]
func execSInterStore(db *DB, args [][]byte) resp.Reply {
	if len(args) < 2 {
		return reply.GetArgNumErrReply("SINTERSTORE")
	}

	destKey := string(args[0])
	srcKeys := make([]string, len(args)-1)
	for i, arg := range args[1:] {
		srcKeys[i] = string(arg)
	}

	allKeys := append([]string{destKey}, srcKeys...)
	// 去重避免重复锁定同一键造成死锁（例如 SINTERSTORE k k）
	sortedKeys := utils.DedupSortedKeys(allKeys)
	locks := make([]*KeyLockHandle, len(sortedKeys))
	for i, key := range sortedKeys {
		locks[i] = db.lockMgr.Lock(key)
	}
	defer func() {
		for _, lock := range locks {
			db.lockMgr.Unlock(lock)
		}
	}()

	sets := make([]*set.Set, 0)
	for _, key := range srcKeys {
		if setObj, exists := db.getAsSet(key); exists {
			if setObj == nil {
				return reply.GetWrongTypeErrReply()
			}
			sets = append(sets, setObj)
		} else {
			db.Remove(destKey)
			db.addAof(utils.ToCmdLineWithName("SINTERSTORE", args...))
			return reply.GetIntReply(0)
		}
	}

	if len(sets) == 0 {
		db.Remove(destKey)
		db.addAof(utils.ToCmdLineWithName("SINTERSTORE", args...))
		return reply.GetIntReply(0)
	}

	result := sets[0]
	for i := 1; i < len(sets); i++ {
		result = result.Intersect(sets[i])
	}

	destSet := set.NewSet()
	for _, m := range result.Members() {
		destSet.Add(m)
	}
	db.PutEntity(destKey, &database.DataEntity{Data: destSet})

	db.addAof(utils.ToCmdLineWithName("SINTERSTORE", args...))
	return reply.GetIntReply(int64(destSet.Len()))
}

// SDIFFSTORE destination key [key ...]
func execSDiffStore(db *DB, args [][]byte) resp.Reply {
	if len(args) < 2 {
		return reply.GetArgNumErrReply("SDIFFSTORE")
	}

	destKey := string(args[0])
	srcKeys := make([]string, len(args)-1)
	for i, arg := range args[1:] {
		srcKeys[i] = string(arg)
	}

	allKeys := append([]string{destKey}, srcKeys...)
	// 去重避免重复锁定同一键造成死锁（例如 SDIFFSTORE k k）
	sortedKeys := utils.DedupSortedKeys(allKeys)
	locks := make([]*KeyLockHandle, len(sortedKeys))
	for i, key := range sortedKeys {
		locks[i] = db.lockMgr.Lock(key)
	}
	defer func() {
		for _, lock := range locks {
			db.lockMgr.Unlock(lock)
		}
	}()

	// The first source key is the base set for diff - if it doesn't exist, result is empty
	firstSet, exists := db.getAsSet(srcKeys[0])
	if isWrongTypeSet(firstSet, exists) {
		return reply.GetWrongTypeErrReply()
	}
	if !exists {
		db.Remove(destKey)
		db.addAof(utils.ToCmdLineWithName("SDIFFSTORE", args...))
		return reply.GetIntReply(0)
	}

	// Subsequent keys are subtracted from the first (missing keys just don't subtract)
	sets := make([]*set.Set, 1, len(srcKeys))
	sets[0] = firstSet
	for i := 1; i < len(srcKeys); i++ {
		if setObj, exists := db.getAsSet(srcKeys[i]); exists {
			if setObj == nil {
				return reply.GetWrongTypeErrReply()
			}
			sets = append(sets, setObj)
		}
	}

	result := sets[0]
	for i := 1; i < len(sets); i++ {
		result = result.Diff(sets[i])
	}

	destSet := set.NewSet()
	for _, m := range result.Members() {
		destSet.Add(m)
	}
	db.PutEntity(destKey, &database.DataEntity{Data: destSet})

	db.addAof(utils.ToCmdLineWithName("SDIFFSTORE", args...))
	return reply.GetIntReply(int64(destSet.Len()))
}

// SSCAN key cursor [MATCH pattern] [COUNT]
func execSScan(db *DB, args [][]byte) resp.Reply {
	if len(args) < 2 {
		return reply.GetArgNumErrReply("SSCAN")
	}

	key := string(args[0])
	cursor, err := strconv.Atoi(string(args[1]))
	if err != nil || cursor < 0 {
		return reply.GetStandardErrorReply("ERR invalid cursor")
	}

	matchPattern := "*"
	count := 10
	for i := 2; i < len(args); i++ {
		arg := string(args[i])
		if arg == "MATCH" && i+1 < len(args) {
			matchPattern = string(args[i+1])
			i++
		} else if arg == "COUNT" && i+1 < len(args) {
			c, _ := strconv.ParseInt(string(args[i+1]), 10, 64)
			count = int(c)
			i++
		}
	}

	var (
		members    []string
		nextCursor int
		errReply   resp.Reply
	)

	db.WithRKeyLock(key, func() {
		setObj, exists := db.getAsSet(key)
		if isWrongTypeSet(setObj, exists) {
			errReply = reply.GetWrongTypeErrReply()
			return
		}
		if !exists {
			return
		}
		members, nextCursor = scanSet(setObj, cursor, matchPattern, count)
	})

	if errReply != nil {
		return errReply
	}

	// 构建返回结果: [cursor, [member1, member2, ...]]
	membersBytes := make([][]byte, len(members))
	for i, m := range members {
		membersBytes[i] = []byte(m)
	}
	return reply.GetScanReply(int64(nextCursor), membersBytes)
}

// SENCODING key
func execSEncoding(db *DB, args [][]byte) resp.Reply {
	if len(args) != 1 {
		return reply.GetArgNumErrReply("SENCODING")
	}

	key := string(args[0])
	var (
		encoding string
		errReply resp.Reply
	)

	db.WithRKeyLock(key, func() {
		setObj, exists := db.getAsSet(key)
		if isWrongTypeSet(setObj, exists) {
			errReply = reply.GetWrongTypeErrReply()
			return
		}
		if !exists {
			return
		}
		switch setObj.Encoding() {
		case 0:
			encoding = "listpack"
		case 1:
			encoding = "dict"
		}
	})

	if errReply != nil {
		return errReply
	}
	if encoding == "" {
		return reply.GetNullBulkReply()
	}
	return reply.GetBulkReply([]byte(encoding))
}

// getAsSet
func (db *DB) getAsSet(key string) (*set.Set, bool) {
	entity, exists := db.GetEntity(key)
	if !exists {
		return nil, false
	}

	setObj, ok := entity.Data.(*set.Set)
	if !ok {
		return nil, true
	}
	return setObj, true
}

func isWrongTypeSet(setObj *set.Set, exists bool) bool {
	return exists && setObj == nil
}

// getOrCreateSet
func (db *DB) getOrCreateSet(key string) (*set.Set, bool) {
	setObj, exists := db.getAsSet(key)
	if exists {
		return setObj, true
	}

	setObj = set.NewSet()
	db.PutEntity(key, &database.DataEntity{Data: setObj})
	return setObj, false
}

// scanSet
func scanSet(s *set.Set, cursor int, pattern string, count int) ([]string, int) {
	members := s.Members()
	if cursor >= len(members) {
		return []string{}, 0
	}

	p := wildcard.CompilePattern(pattern)

	result := make([]string, 0)
	matched := 0
	i := cursor

	for i < len(members) && matched < count {
		if p.IsMatch(members[i]) {
			result = append(result, members[i])
			matched++
		}
		i++
	}

	if i >= len(members) {
		return result, 0
	}
	return result, i
}

// init
func init() {
	RegisterCommand("SADD", execSAdd, -3)
	RegisterCommand("SREM", execSRem, -3)
	RegisterCommand("SISMEMBER", execSIsMember, 3)
	RegisterCommand("SMEMBERS", execSMembers, 2)
	RegisterCommand("SCARD", execSCard, 2)
	RegisterCommand("SPOP", execSPop, -2)
	RegisterCommand("SRANDMEMBER", execSRandMember, -2)
	RegisterCommand("SMOVE", execSMove, 4)
	RegisterCommand("SUNION", execSUnion, -2)
	RegisterCommand("SINTER", execSInter, -2)
	RegisterCommand("SDIFF", execSDiff, -2)
	RegisterCommand("SUNIONSTORE", execSUnionStore, -3)
	RegisterCommand("SINTERSTORE", execSInterStore, -3)
	RegisterCommand("SDIFFSTORE", execSDiffStore, -3)
	RegisterCommand("SSCAN", execSScan, -3)
	RegisterCommand("SENCODING", execSEncoding, 2)
}
