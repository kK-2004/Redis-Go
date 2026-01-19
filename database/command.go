package database

import "strings"

var cmdTable = make(map[string]*command)

type command struct {
	exec   ExecFunc
	argCnt int
}

func RegisterCommand(name string, exec ExecFunc, argCnt int) {
	cmdTable[strings.ToLower(name)] = &command{
		exec:   exec,
		argCnt: argCnt,
	}
}
