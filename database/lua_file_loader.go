package database

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"Redis_Go/lib/logger"
)

// ScriptLoader .lua 脚本文件加载器
type ScriptLoader struct {
	dir       string              // 脚本目录
	scripts   map[string]string   // name -> script content
	nameToSHA map[string]string   // name -> SHA1
}

// NewScriptLoader 创建新的脚本加载器
func NewScriptLoader(dir string) *ScriptLoader {
	if dir == "" {
		dir = "./scripts"
	}
	return &ScriptLoader{
		dir:       dir,
		scripts:   make(map[string]string),
		nameToSHA: make(map[string]string),
	}
}

// Load 加载目录中的所有.lua文件
func (sl *ScriptLoader) Load(cache *ScriptCache) error {
	// 检查目录是否存在
	info, err := os.Stat(sl.dir)
	if err != nil {
		if os.IsNotExist(err) {
			logger.Info("Script directory does not exist: " + sl.dir)
			return nil // 不存在不是错误，只是没有脚本可加载
		}
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("script path is not a directory: %s", sl.dir)
	}

	// 遍历目录
	files, err := os.ReadDir(sl.dir)
	if err != nil {
		return err
	}

	for _, file := range files {
		// 只处理.lua文件
		if file.IsDir() || !strings.HasSuffix(file.Name(), ".lua") {
			continue
		}

		// 读取文件内容
		filePath := filepath.Join(sl.dir, file.Name())
		content, err := os.ReadFile(filePath)
		if err != nil {
			logger.Error("Failed to read script file: " + filePath + ", error: " + err.Error())
			continue
		}

		// 提取脚本名称 (去掉.lua后缀)
		name := strings.TrimSuffix(file.Name(), ".lua")
		script := string(content)

		// 编译并缓存脚本
		sha1, err := cache.Set(script)
		if err != nil {
			logger.Error("Failed to compile script file: " + filePath + ", error: " + err.Error())
			continue
		}

		// 保存映射
		sl.scripts[name] = script
		sl.nameToSHA[name] = sha1

		logger.Info("Loaded script: " + name + " (SHA1: " + sha1 + ")")
	}

	return nil
}

// GetByName 根据名称获取脚本内容
func (sl *ScriptLoader) GetByName(name string) (string, bool) {
	script, ok := sl.scripts[name]
	return script, ok
}

// GetSHAByName 根据名称获取脚本的SHA1
func (sl *ScriptLoader) GetSHAByName(name string) (string, bool) {
	sha1, ok := sl.nameToSHA[name]
	return sha1, ok
}

// GetScriptNames 获取所有已加载的脚本名称
func (sl *ScriptLoader) GetScriptNames() []string {
	names := make([]string, 0, len(sl.scripts))
	for name := range sl.scripts {
		names = append(names, name)
	}
	return names
}
