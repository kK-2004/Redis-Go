package database

import (
	"sync"

	lua "github.com/yuin/gopher-lua"
)

// VMPool Lua VM 池，用于复用 LState 实例
type VMPool struct {
	pool sync.Pool
}

// NewVMPool 创建新的 VM 池
func NewVMPool() *VMPool {
	return &VMPool{
		pool: sync.Pool{
			New: func() interface{} {
				return newVM()
			},
		},
	}
}

// Get 从池中获取一个 VM 实例
func (p *VMPool) Get() *lua.LState {
	vm := p.pool.Get().(*lua.LState)
	return vm
}

// Put 将 VM 实例放回池中
func (p *VMPool) Put(vm *lua.LState) {
	// 重置 VM 的全局状态
	p.resetVM(vm)
	p.pool.Put(vm)
}

// newVM 创建并配置一个新的 Lua VM
func newVM() *lua.LState {
	L := lua.NewState(lua.Options{
		CallStackSize:       256,
		RegistrySize:        512,
		RegistryMaxSize:     1024,
		RegistryGrowStep:    32,
		SkipOpenLibs:        false,
		IncludeGoStackTrace: false,
	})

	// 开启标准库
	for _, pair := range []struct {
		n string
		f lua.LGFunction
	}{
		{lua.LoadLibName, lua.OpenPackage},
		{lua.BaseLibName, lua.OpenBase},
		{lua.TabLibName, lua.OpenTable},
		{lua.StringLibName, lua.OpenString},
		{lua.MathLibName, lua.OpenMath},
		{lua.DebugLibName, lua.OpenDebug},
	} {
		if err := L.CallByParam(lua.P{
			Fn:      L.NewFunction(pair.f),
			NRet:    0,
			Protect: true,
		}, lua.LString(pair.n)); err != nil {
			panic(err)
		}
	}

	return L
}

// resetVM 重置 VM 的状态，清除脚本相关的全局变量
func (p *VMPool) resetVM(vm *lua.LState) {
	// 清除 redis 全局对象（脚本执行时创建）
	vm.SetGlobal("redis", lua.LNil)

	// 清除 KEYS 和 ARGV 全局变量
	vm.SetGlobal("KEYS", lua.LNil)
	vm.SetGlobal("ARGV", lua.LNil)

	// 清理注册表中的临时条目（防止内存泄漏）
	// 注意：不清理标准库注册的内容
	vm.SetTop(0)
}
