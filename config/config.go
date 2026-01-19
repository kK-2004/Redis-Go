package config

import (
	"Redis_Go/lib/logger"
	"bufio"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"
)

type ServerProperties struct {
	Bind               string   `cfg:"bind"`
	Port               int      `cfg:"port"`
	AppendOnly         bool     `cfg:"appendOnly"`
	AppendOnlyFilename string   `cfg:"appendOnlyFilename"`
	MaxClients         int      `cfg:"maxClients"`
	RequirePass        string   `cfg:"requirePass"`
	Databases          int      `cfg:"databases"`
	Peers              []string `cfg:"peers"`
	Self               string   `cfg:"self"`
}

var Properties *ServerProperties

func init() {
	Properties = &ServerProperties{
		Bind:       "127.0.0.1",
		Port:       6666,
		AppendOnly: false,
	}
}

func SetupConfig(configFileName string) {
	file, err := os.Open(configFileName)
	if err != nil {
		panic(err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
		}
	}(file)
	Properties = parse(file)
}

func parse(src io.Reader) *ServerProperties {
	config := &ServerProperties{}

	rawMap := make(map[string]string)
	scanner := bufio.NewScanner(src)
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) > 0 && line[0] == '#' {
			continue
		}
		pivot := strings.IndexAny(line, " ")
		if pivot > 0 && pivot < len(line)-1 {
			key := line[0:pivot]
			val := strings.Trim(line[pivot+1:], " ")
			rawMap[strings.ToLower(key)] = val
		}
	}
	if err := scanner.Err(); err != nil {
		logger.Fatal(err)
	}

	t := reflect.TypeOf(config)
	v := reflect.ValueOf(config)
	n := t.Elem().NumField()
	for i := 0; i < n; i++ {
		field := t.Elem().Field(i)
		fieldVal := v.Elem().Field(i)
		key, ok := field.Tag.Lookup("cfg")
		if !ok {
			key = field.Name
		}
		value, ok := rawMap[strings.ToLower(key)]
		if ok {
			// fill config
			switch field.Type.Kind() {
			case reflect.String:
				fieldVal.SetString(value)
			case reflect.Int:
				intValue, err := strconv.ParseInt(value, 10, 64)
				if err == nil {
					fieldVal.SetInt(intValue)
				}
			case reflect.Bool:
				boolValue := "yes" == value || "true" == value
				fieldVal.SetBool(boolValue)
			case reflect.Slice:
				if field.Type.Elem().Kind() == reflect.String {
					slice := strings.Split(value, ",")
					fieldVal.Set(reflect.ValueOf(slice))
				}
			default:
				panic("unhandled default case")
			}
		}
	}
	return config
}
