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

type KVDB struct {
	datas map[string]string
}

func (k *KVDB) Init() {
	k.datas = map[string]string{}
}
func (k *KVDB) Process(in string) (out string, agree bool, err error) {
	log.Printf("KVDB: process: %s\n", in)
	if x, legal := k.parser(in); !legal {
		return "db: illegal operation", false, nil
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

func (k *KVDB) UndoProcess(in string) (out string, agree bool, err error) {
	fmt.Printf("============================== !!%s\n", in)
	return in, true, nil
}

func (k *KVDB) parser(order string) (op, bool) {
	res := strings.SplitN(order, " ", 3)
	if res[0] == "read" {
		return op{opType: read, data: kvData{key: res[1]}}, true
	} else if res[0] == "write" {
		return op{opType: write, data: kvData{key: res[1], val: res[2]}}, true
	} else {
		return op{}, false
	}
}

func (k *KVDB) ToString() string {
	u := "kvdb: "
	for i, v := range k.datas {
		u += fmt.Sprintf("\nk: %s v: %s", i, v)
	}
	return u
}

// 需要自己的文件系统，同时可以从日志中恢复
