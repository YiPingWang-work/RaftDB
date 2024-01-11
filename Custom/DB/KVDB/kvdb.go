package KVDB

import (
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"
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

func (k *KVDB) Init() {
	k.data = map[string]string{}
	k.ChangeProcessDelay(0, false)
}
func (k *KVDB) Process(in string) (out string, agree bool, err error) {
	log.Printf("KVDB: process: %s\n", in)
	time.Sleep(k.delay)
	if x, legal := k.parser(in); !legal {
		return "db: illegal operation", false, nil
	} else {
		if x.opType == read {
			if res, ok := k.data[x.data.key]; ok {
				return "val: " + res, true, nil
			} else {
				return "(empty)", true, nil
			}
		} else if x.opType == write {
			k.data[x.data.key] = x.data.val
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
	if res[0] == "read" && len(res) == 2 {
		return op{opType: read, data: kvData{key: res[1]}}, true
	} else if res[0] == "write" && len(res) == 3 {
		return op{opType: write, data: kvData{key: res[1], val: res[2]}}, true
	} else {
		return op{}, false
	}
}

func (k *KVDB) ChangeProcessDelay(delay int, random bool) {
	if !random {
		k.delay = time.Duration(delay) * time.Millisecond
	} else {
		k.delay = time.Duration(rand.Intn(delay)) * time.Millisecond
	}
}

func (k *KVDB) ToString() string {
	u := "kvdb: "
	for i, v := range k.data {
		u += fmt.Sprintf("\nk: %s v: %s", i, v)
	}
	return u
}

// 可以从日志中恢复
