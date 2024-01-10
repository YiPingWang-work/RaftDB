package KVDB

import (
	"fmt"
	"log"
	"strings"
)

const (
	write = iota
	read
	add1
	sub1
	readAll
)

type kvData struct {
	key string
	val string
}

type op struct {
	opType int
	data   kvData
}

type kvdb struct {
	datas map[string]string
}

func (k *kvdb) Init() {
	k.datas = map[string]string{}
}
func (k *kvdb) Process(in string) (out string, agree bool, err error) {
	if x, legal := k.parser(in); !legal {
		return "illegal operation", false, nil
	} else {
		if x.opType == read {
			if res, ok := k.datas[x.data.key]; ok {
				return "val: " + res, true, nil
			} else {
				return "(empty)", true, nil
			}
		} else if x.opType == write {
			k.datas[x.data.key] = x.data.val
			return "write successfully, key: " + x.data.key + ", value: " + x.data.val, true, nil
		}
	}
	return in, false, nil
}

func (k *kvdb) UndoProcess(in string) (out string, agree bool, err error) {
	fmt.Printf("============================== !!%s\n", in)
	return in, true, nil
}

func (k *kvdb) parser(order string) (op, bool) {
	res := strings.SplitN(order, " ", 3)
	log.Println(res)
	if res[0] == "read" {
		return op{opType: read, data: kvData{key: res[1]}}, true
	} else if res[0] == "write" {
		return op{opType: write, data: kvData{key: res[1], val: res[2]}}, true
	} else {
		return op{}, false
	}
}

// 需要自己的文件系统，同时可以从日志中恢复
