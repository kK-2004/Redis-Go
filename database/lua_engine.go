package database

import (
	"strconv"

	lua "github.com/yuin/gopher-lua"

	"Redis_Go/interface/resp"
	"Redis_Go/resp/reply"
)

// LuaEngine Lua脚本执行引擎
type LuaEngine struct {
	db          *DB
	vmPool      *VMPool
	scriptCache *ScriptCache
}

// NewLuaEngine 创建新的Lua引擎
func NewLuaEngine(db *DB) *LuaEngine {
	return &LuaEngine{
		db:          db,
		vmPool:      NewVMPool(),
		scriptCache: NewScriptCache(),
	}
}

// WithDB 返回绑定到指定 DB 的 LuaEngine 视图，复用脚本缓存和 VM 池
func (e *LuaEngine) WithDB(db *DB) *LuaEngine {
	if db == nil {
		return e
	}
	return &LuaEngine{
		db:          db,
		vmPool:      e.vmPool,
		scriptCache: e.scriptCache,
	}
}

// Eval 执行Lua脚本
func (e *LuaEngine) Eval(script string, keys, args []string) resp.Reply {
	// 编译或获取缓存的脚本
	proto, _, err := e.scriptCache.GetOrCompile(script)
	if err != nil {
		return reply.GetStandardErrorReply("ERR Error compiling script (new call): " + err.Error())
	}

	return e.execute(proto, keys, args)
}

// EvalBySHA 通过SHA1执行缓存的脚本
func (e *LuaEngine) EvalBySHA(sha1Hash string, keys, args []string) resp.Reply {
	proto, ok := e.scriptCache.Get(sha1Hash)
	if !ok {
		return reply.GetStandardErrorReply("NOSCRIPT No matching script. Please use EVAL.")
	}

	return e.execute(proto, keys, args)
}

// execute 执行编译后的脚本
func (e *LuaEngine) execute(proto *lua.FunctionProto, keys, args []string) resp.Reply {
	// 预锁定所有键
	lockHandle := e.db.lockMgr.LockKeys(keys)
	defer e.db.lockMgr.UnlockKeys(lockHandle)

	// 获取VM
	vm := e.vmPool.Get()
	defer e.vmPool.Put(vm)

	// 注册 redis.call/pcall 等函数
	e.registerRedisFunctions(vm)

	// 设置全局变量
	e.setGlobals(vm, keys, args)

	// 执行脚本
	luaFunc := vm.NewFunctionFromProto(proto)
	vm.Push(luaFunc)
	if err := vm.PCall(0, 1, nil); err != nil {
		return reply.GetStandardErrorReply("ERR Error running script (call to " + proto.SourceName + "): " + err.Error())
	}

	// 获取返回值
	ret := vm.Get(-1)
	vm.Pop(1)

	// 转换为Redis回复
	return e.convertLuaToReply(vm, ret)
}

// setGlobals 设置Lua全局变量KEYS和ARGV
func (e *LuaEngine) setGlobals(vm *lua.LState, keys, args []string) {
	// 设置 KEYS
	keysTable := vm.CreateTable(len(keys), 0)
	for i, k := range keys {
		keysTable.RawSetInt(i+1, lua.LString(k)) // Lua数组从1开始
	}
	vm.SetGlobal("KEYS", keysTable)

	// 设置 ARGV
	argvTable := vm.CreateTable(len(args), 0)
	for i, a := range args {
		argvTable.RawSetInt(i+1, lua.LString(a))
	}
	vm.SetGlobal("ARGV", argvTable)
}

// registerRedisFunctions 注册redis.call和redis.pcall函数
func (e *LuaEngine) registerRedisFunctions(vm *lua.LState) {
	// 创建redis表
	redisTable := vm.NewTable()

	// redis.call - 错误时抛出异常
	vm.SetField(redisTable, "call", vm.NewFunction(func(L *lua.LState) int {
		result := e.executeRedisCommand(L, true)
		L.Push(result)
		return 1
	}))

	// redis.pcall - 错误时返回错误对象
	vm.SetField(redisTable, "pcall", vm.NewFunction(func(L *lua.LState) int {
		result := e.executeRedisCommand(L, false)
		L.Push(result)
		return 1
	}))

	// redis.error_reply - 创建错误回复
	vm.SetField(redisTable, "error_reply", vm.NewFunction(func(L *lua.LState) int {
		msg := L.CheckString(1)
		tbl := L.NewTable()
		L.SetField(tbl, "err", lua.LString(msg))
		L.Push(tbl)
		return 1
	}))

	// redis.status_reply - 创建状态回复
	vm.SetField(redisTable, "status_reply", vm.NewFunction(func(L *lua.LState) int {
		msg := L.CheckString(1)
		tbl := L.NewTable()
		L.SetField(tbl, "ok", lua.LString(msg))
		L.Push(tbl)
		return 1
	}))

	vm.SetGlobal("redis", redisTable)
}

// executeRedisCommand 在Lua中执行Redis命令
func (e *LuaEngine) executeRedisCommand(L *lua.LState, raiseError bool) lua.LValue {
	// 获取命令名
	cmd := L.CheckString(1)

	// 获取参数
	n := L.GetTop()
	args := make([][]byte, 0, n-1)
	for i := 2; i <= n; i++ {
		arg := L.Get(i)
		args = append(args, []byte(e.luaValueToString(arg)))
	}

	// 构建命令行
	cmdLine := append([][]byte{[]byte(cmd)}, args...)

	// 执行命令 (锁已由execute获取，使用无锁版本避免死锁)
	result := e.db.ExecWithoutLock(cmdLine)

	// 检查是否为错误
	if reply.IsErrReply(result) {
		errBytes := result.ToBytes()
		errMsg := string(errBytes)
		if len(errMsg) > 2 {
			errMsg = errMsg[1 : len(errMsg)-2] // 去掉-前缀和\r\n后缀
		}
		if raiseError {
			// 抛出Lua错误
			L.RaiseError(errMsg)
		}
		// 返回错误对象
		tbl := L.NewTable()
		L.SetField(tbl, "err", lua.LString(errMsg))
		return tbl
	}

	// 转换结果为Lua值
	return e.replyToLuaValue(L, result)
}

// convertLuaToReply 将Lua值转换为Redis回复
func (e *LuaEngine) convertLuaToReply(vm *lua.LState, value lua.LValue) resp.Reply {
	switch v := value.(type) {
	case lua.LNumber:
		return reply.GetIntReply(int64(v))
	case lua.LString:
		return reply.GetBulkReply([]byte(v))
	case *lua.LTable:
		// 检查是否为特殊回复 (ok/err字段)
		okField := v.RawGetString("ok")
		if okField != lua.LNil {
			return reply.GetStatusReply(lua.LVAsString(okField))
		}
		errField := v.RawGetString("err")
		if errField != lua.LNil {
			return reply.GetStandardErrorReply(lua.LVAsString(errField))
		}

		// 普通数组
		return e.tableToMultiBulkReply(v)
	case lua.LBool:
		if v {
			return reply.GetIntReply(1)
		}
		return reply.GetNullBulkReply()
	case *lua.LNilType:
		return reply.GetNullBulkReply()
	default:
		return reply.GetBulkReply([]byte(lua.LVAsString(value)))
	}
}

// tableToMultiBulkReply 将Lua表转换为MultiBulkReply
func (e *LuaEngine) tableToMultiBulkReply(tbl *lua.LTable) resp.Reply {
	var args [][]byte

	// 遍历数组部分 (Lua数组从1开始)
	tblLen := tbl.Len()
	for i := 1; i <= tblLen; i++ {
		val := tbl.RawGetInt(i)
		args = append(args, e.luaValueToBytes(val))
	}

	// 如果没有数组部分，检查哈希部分
	if len(args) == 0 {
		tbl.ForEach(func(k, v lua.LValue) {
			args = append(args, e.luaValueToBytes(k))
			args = append(args, e.luaValueToBytes(v))
		})
	}

	return reply.GetMultiBulkReply(args)
}

// replyToLuaValue 将Redis回复转换为Lua值
func (e *LuaEngine) replyToLuaValue(L *lua.LState, r resp.Reply) lua.LValue {
	switch v := r.(type) {
	case *reply.IntReply:
		return lua.LNumber(v.Code)
	case *reply.BulkReply:
		if v.Arg == nil {
			return lua.LFalse
		}
		return lua.LString(string(v.Arg))
	case *reply.MultiBulkReply:
		tbl := L.CreateTable(len(v.Args), 0)
		for i, arg := range v.Args {
			if arg == nil {
				tbl.RawSetInt(i+1, lua.LFalse)
			} else {
				tbl.RawSetInt(i+1, lua.LString(string(arg)))
			}
		}
		return tbl
	case *reply.StatusReply:
		tbl := L.NewTable()
		L.SetField(tbl, "ok", lua.LString(v.Status))
		return tbl
	case *reply.StandardErrorReply:
		tbl := L.NewTable()
		L.SetField(tbl, "err", lua.LString(v.Status))
		return tbl
	case *reply.ScanReply:
		// 返回 [cursor, [members...]]
		tbl := L.CreateTable(2, 0)
		tbl.RawSetInt(1, lua.LNumber(v.Cursor))
		members := L.CreateTable(len(v.Members), 0)
		for i, m := range v.Members {
			if m == nil {
				members.RawSetInt(i+1, lua.LFalse)
			} else {
				members.RawSetInt(i+1, lua.LString(string(m)))
			}
		}
		tbl.RawSetInt(2, members)
		return tbl
	default:
		bytes := r.ToBytes()
		if len(bytes) > 0 && bytes[0] == ':' {
			// Integer reply
			code, _ := strconv.ParseInt(string(bytes[1:len(bytes)-2]), 10, 64)
			return lua.LNumber(code)
		}
		if len(bytes) > 0 && bytes[0] == '+' {
			// Status reply
			tbl := L.NewTable()
			L.SetField(tbl, "ok", lua.LString(string(bytes[1:len(bytes)-2])))
			return tbl
		}
		return lua.LString(string(bytes))
	}
}

// luaValueToString 将Lua值转换为字符串
func (e *LuaEngine) luaValueToString(v lua.LValue) string {
	switch val := v.(type) {
	case lua.LString:
		return string(val)
	case lua.LNumber:
		return strconv.FormatFloat(float64(val), 'f', -1, 64)
	case lua.LBool:
		if val {
			return "1"
		}
		return "0"
	case *lua.LNilType:
		return ""
	default:
		return lua.LVAsString(v)
	}
}

// luaValueToBytes 将Lua值转换为字节切片
func (e *LuaEngine) luaValueToBytes(v lua.LValue) []byte {
	return []byte(e.luaValueToString(v))
}

// GetScriptCache 获取脚本缓存 (用于SCRIPT命令)
func (e *LuaEngine) GetScriptCache() *ScriptCache {
	return e.scriptCache
}

// CompileScript 编译脚本并返回SHA1 (用于SCRIPT LOAD)
func (e *LuaEngine) CompileScript(script string) (string, error) {
	return e.scriptCache.Set(script)
}

// ScriptExists 检查脚本是否存在 (用于SCRIPT EXISTS)
func (e *LuaEngine) ScriptExists(sha1Hashes ...string) []int {
	result := make([]int, len(sha1Hashes))
	for i, sha1 := range sha1Hashes {
		if e.scriptCache.Exists(sha1) {
			result[i] = 1
		} else {
			result[i] = 0
		}
	}
	return result
}

// FlushScripts 清空脚本缓存 (用于SCRIPT FLUSH)
func (e *LuaEngine) FlushScripts() {
	e.scriptCache.Flush()
}
