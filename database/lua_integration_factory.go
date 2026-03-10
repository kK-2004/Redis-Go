package database

// InitLuaEngineOnDatabases 初始化 Lua 引擎
// 在数据库创建后调用，对传入的数据库进行初始化
func InitLuaEngineOnDatabases(databases *Database) {
    if databases == nil || len(databases.dbSet) == 0 {
        return
    }

    // 使用第一个 DB 宥例初始化全局 Lua 引擎
    if firstDB, ok := databases.dbSet[0].(*DB); ok {
        InitLuaEngine(firstDB)
    }
}
