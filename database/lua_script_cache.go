package database

import (
	"crypto/sha1"
	"encoding/hex"
	"strings"
	"sync"

	lua "github.com/yuin/gopher-lua"
	"github.com/yuin/gopher-lua/parse"
)

// ScriptCache Lua脚本缓存，使用SHA1作为键存储编译后的脚本
type ScriptCache struct {
	mu      sync.RWMutex
	scripts map[string]*lua.FunctionProto // SHA1 -> 编译后的Proto
}

// NewScriptCache 创建新的脚本缓存
func NewScriptCache() *ScriptCache {
	return &ScriptCache{
		scripts: make(map[string]*lua.FunctionProto),
	}
}

// GetOrCompile 获取缓存的脚本，如果不存在则编译并缓存
// 返回: 编译后的Proto, SHA1, error
func (sc *ScriptCache) GetOrCompile(script string) (*lua.FunctionProto, string, error) {
	sha1Hash := sc.calculateSHA1(script)

	// 先尝试读锁获取
	sc.mu.RLock()
	proto, ok := sc.scripts[sha1Hash]
	sc.mu.RUnlock()
	if ok {
		return proto, sha1Hash, nil
	}

	// 获取写锁进行编译和缓存
	sc.mu.Lock()
	defer sc.mu.Unlock()

	// 双重检查：可能在等待写锁时已被其他 goroutine 编译
	if proto, ok := sc.scripts[sha1Hash]; ok {
		return proto, sha1Hash, nil
	}

	// 编译脚本
	compiled, err := sc.compile(script, sha1Hash)
	if err != nil {
		return nil, sha1Hash, err
	}

	// 缓存
	sc.scripts[sha1Hash] = compiled
	return compiled, sha1Hash, nil
}

// Get 根据SHA1获取缓存的脚本
func (sc *ScriptCache) Get(sha1Hash string) (*lua.FunctionProto, bool) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	proto, ok := sc.scripts[sha1Hash]
	return proto, ok
}

// Set 编译并缓存脚本，返回SHA1
func (sc *ScriptCache) Set(script string) (string, error) {
	sha1Hash := sc.calculateSHA1(script)

	// 检查是否已存在
	sc.mu.RLock()
	_, ok := sc.scripts[sha1Hash]
	sc.mu.RUnlock()
	if ok {
		return sha1Hash, nil
	}

	// 获取写锁进行编译和缓存
	sc.mu.Lock()
	defer sc.mu.Unlock()

	// 双重检查
	if _, ok := sc.scripts[sha1Hash]; ok {
		return sha1Hash, nil
	}

	// 编译脚本
	compiled, err := sc.compile(script, sha1Hash)
	if err != nil {
		return sha1Hash, err
	}

	// 缓存
	sc.scripts[sha1Hash] = compiled
	return sha1Hash, nil
}

// Exists 检查SHA1对应的脚本是否存在
func (sc *ScriptCache) Exists(sha1Hash string) bool {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	_, ok := sc.scripts[sha1Hash]
	return ok
}

// Flush 清空所有缓存的脚本
func (sc *ScriptCache) Flush() {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.scripts = make(map[string]*lua.FunctionProto)
}

// compile 编译Lua脚本
func (sc *ScriptCache) compile(script, name string) (*lua.FunctionProto, error) {
	reader := strings.NewReader(script)
	chunk, err := parse.Parse(reader, name)
	if err != nil {
		return nil, err
	}
	proto, err := lua.Compile(chunk, name)
	if err != nil {
		return nil, err
	}
	return proto, nil
}

// calculateSHA1 计算脚本的SHA1哈希
func (sc *ScriptCache) calculateSHA1(script string) string {
	h := sha1.Sum([]byte(script))
	return hex.EncodeToString(h[:])
}
