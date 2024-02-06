package KVDB

import (
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"
)

/*
随便写了一个键值数据库
*/

const (
	write = iota
	read
	watch
)

type kvData struct {
	key string
	val string
}

type op struct { // 原神怎么你了
	opType int
	data   kvData
}

/*
type App interface {
	Process(in string) (out string, agreeNext bool, err error)
	UndoProcess(in string) (out string, agreeNext bool, err error) // 处理逆信息
	ChangeProcessDelay(delay int, random bool)
	Init()
	ToString() string
}
*/

type KVDB struct {
	data  map[string]string
	delay time.Duration
}

func (k *KVDB) Init() func(string) (bool, string, string) {
	k.data = map[string]string{}
	k.ChangeProcessDelay(0, false)
	return func(order string) (bool, string, string) {
		if op, ok := k.parser(order); ok && op.opType == write {
			return true, "write" + op.data.key, op.data.val
		}
		return false, "", ""
	}
}
func (k *KVDB) Process(in string) (out string, agree bool, watching bool, err error) {
	log.Printf("KVDB: process: %s\n", in)
	time.Sleep(k.delay)
	if x, legal := k.parser(in); !legal {
		return "db: illegal operation", false, false, nil
	} else {
		if x.opType == read {
			if res, ok := k.data[x.data.key]; ok {
				return res, true, false, nil
			} else {
				return "(empty)", true, false, nil
			}
		} else if x.opType == write {
			k.data[x.data.key] = x.data.val
			return "write successfully: " + x.data.key + ", " + x.data.val, true, false, nil
		} else if x.opType == watch {
			return x.data.key, true, true, nil
		}
	}
	return in, false, false, nil
}

func (k *KVDB) UndoProcess(in string) (out string, agree bool, err error) {
	fmt.Printf("!! redo %s\n", in)
	return in, true, nil
}

func (k *KVDB) parser(order string) (op, bool) {
	res := strings.Split(order, "'")
	if len(res) == 2 && res[0] == "read" {
		return op{opType: read, data: kvData{key: res[1]}}, true
	} else if len(res) == 3 && res[0] == "write" {
		return op{opType: write, data: kvData{key: res[1], val: res[2]}}, true
	} else if len(res) == 3 && res[0] == "watch" && res[1] == "write" {
		return op{opType: watch, data: kvData{key: res[1] + res[2]}}, true
	}
	return op{}, false
}

func (k *KVDB) ChangeProcessDelay(delay int, random bool) {
	if !random {
		k.delay = time.Duration(delay) * time.Millisecond
	} else {
		k.delay = time.Duration(rand.Intn(delay)) * time.Millisecond
	}
}

func (k *KVDB) ToString() string {
	u := fmt.Sprintf("kvdb: %d", len(k.data))
	for i, v := range k.data {
		u += fmt.Sprintf("\nk: %s v: %s", i, v)
	}
	return u
}

// 可以从日志中恢复
