package database

import (
	"strconv"
	"strings"

	"Redis_Go/interface/resp"
	"Redis_Go/resp/reply"
)

// 全局Lua引擎实例
var globalLuaEngine *LuaEngine

// InitLuaEngine 初始化全局Lua引擎
func InitLuaEngine(db *DB) {
	globalLuaEngine = NewLuaEngine(db)
}

// init 函数注册命令
func init() {
	// 注册 EVAL 命令
	RegisterCommand("eval", execEval, -3) // -3 表示至少需要3个参数: script numkeys ...
	// 注册 EVALSHA 命令
	RegisterCommand("evalsha", execEvalSHA, -3)
	// 注册 SCRIPT 命令
	RegisterCommand("script", execScript, -2) // SCRIPT subcommand ...
}

// execEval 执行 EVAL 命令
// EVAL script numkeys key [key ...] arg [arg ...]
func execEval(db *DB, args [][]byte) resp.Reply {
	if globalLuaEngine == nil {
		return reply.GetStandardErrorReply("ERR Lua engine not initialized")
	}

	// 至少需要 script 和 numkeys
	if len(args) < 2 {
		return reply.GetArgNumErrReply("eval")
	}

	script := string(args[0])
	numKeys, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return reply.GetStandardErrorReply("ERR value is not an integer or out of range")
	}
	if numKeys < 0 {
		return reply.GetStandardErrorReply("ERR Number of keys can't be negative")
	}
	if numKeys > len(args)-2 {
		return reply.GetStandardErrorReply("ERR Number of keys can't be greater than number of args")
	}

	// 检查参数数量
	if len(args) < 2+numKeys {
		return reply.GetArgNumErrReply("eval")
	}

	// 提取 keys
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = string(args[2+i])
	}

	// 提取 args
	argCount := len(args) - 2 - numKeys
	luaArgs := make([]string, argCount)
	for i := 0; i < argCount; i++ {
		luaArgs[i] = string(args[2+numKeys+i])
	}

	return globalLuaEngine.Eval(script, keys, luaArgs)
}

// execEvalSHA 执行 EVALSHA 命令
// EVALSHA sha1 numkeys key [key ...] arg [arg ...]
func execEvalSHA(db *DB, args [][]byte) resp.Reply {
	if globalLuaEngine == nil {
		return reply.GetStandardErrorReply("ERR Lua engine not initialized")
	}

	// 至少需要 sha1 和 numkeys
	if len(args) < 2 {
		return reply.GetArgNumErrReply("evalsha")
	}

	sha1Hash := string(args[0])
	numKeys, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return reply.GetStandardErrorReply("ERR value is not an integer or out of range")
	}
	if numKeys < 0 {
		return reply.GetStandardErrorReply("ERR Number of keys can't be negative")
	}
	if numKeys > len(args)-2 {
		return reply.GetStandardErrorReply("ERR Number of keys can't be greater than number of args")
	}

	// 检查参数数量
	if len(args) < 2+numKeys {
		return reply.GetArgNumErrReply("evalsha")
	}

	// 提取 keys
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = string(args[2+i])
	}

	// 提取 args
	argCount := len(args) - 2 - numKeys
	luaArgs := make([]string, argCount)
	for i := 0; i < argCount; i++ {
		luaArgs[i] = string(args[2+numKeys+i])
	}

	return globalLuaEngine.EvalBySHA(sha1Hash, keys, luaArgs)
}

// execScript 执行 SCRIPT 命令
// SCRIPT <subcommand> [arg ...]
func execScript(db *DB, args [][]byte) resp.Reply {
	if globalLuaEngine == nil {
		return reply.GetStandardErrorReply("ERR Lua engine not initialized")
	}

	if len(args) < 1 {
		return reply.GetArgNumErrReply("script")
	}

	subcommand := strings.ToLower(string(args[0]))

	switch subcommand {
	case "load":
		return execScriptLoad(args[1:])
	case "exists":
		return execScriptExists(args[1:])
	case "flush":
		return execScriptFlush()
	default:
		return reply.GetStandardErrorReply("ERR Unknown SCRIPT subcommand: " + subcommand)
	}
}

// execScriptLoad 执行 SCRIPT LOAD 命令
// SCRIPT LOAD script
func execScriptLoad(args [][]byte) resp.Reply {
	if len(args) < 1 {
		return reply.GetArgNumErrReply("script load")
	}

	script := string(args[0])
	sha1, err := globalLuaEngine.CompileScript(script)
	if err != nil {
		return reply.GetStandardErrorReply("ERR Error compiling script (new call): " + err.Error())
	}

	return reply.GetBulkReply([]byte(sha1))
}

// execScriptExists 执行 SCRIPT EXISTS 命令
// SCRIPT EXISTS sha1 [sha1 ...]
func execScriptExists(args [][]byte) resp.Reply {
	if len(args) < 1 {
		return reply.GetArgNumErrReply("script exists")
	}

	sha1Hashes := make([]string, len(args))
	for i, arg := range args {
		sha1Hashes[i] = string(arg)
	}

	results := globalLuaEngine.ScriptExists(sha1Hashes...)
	resultBytes := make([][]byte, len(results))
	for i, r := range results {
		resultBytes[i] = []byte(strconv.Itoa(r))
	}

	return reply.GetMultiBulkReply(resultBytes)
}

// execScriptFlush 执行 SCRIPT FLUSH 命令
// SCRIPT FLUSH
func execScriptFlush() resp.Reply {
	globalLuaEngine.FlushScripts()
	return reply.GetStatusReply("OK")
}
