package utils

import (
	"sort"
	"strconv"
)

func String2Cmdline(cmd ...string) [][]byte {
	args := make([][]byte, len(cmd))
	for idx, v := range cmd {
		args[idx] = []byte(v)
	}
	return args
}

func ToCmdLineWithName(name string, args ...[]byte) [][]byte {
	cmd := make([][]byte, len(args)+1)
	cmd[0] = []byte(name)
	for i, s := range args {
		cmd[i+1] = s
	}
	return cmd
}

// ParseInt 解析字符串为 int64
func ParseInt(s string) int64 {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

// SortedKeys 返回排序后的键列表
func SortedKeys(keys []string) []string {
	sorted := make([]string, len(keys))
	copy(sorted, keys)
	sort.Strings(sorted)
	return sorted
}

// DedupSortedKeys 返回排序并去重后的键列表
func DedupSortedKeys(keys []string) []string {
	if len(keys) == 0 {
		return keys
	}
	sorted := make([]string, len(keys))
	copy(sorted, keys)
	sort.Strings(sorted)

	// 去重
	result := sorted[:1]
	for i := 1; i < len(sorted); i++ {
		if sorted[i] != sorted[i-1] {
			result = append(result, sorted[i])
		}
	}
	return result
}
