package utils

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
