package Log

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

// 必须保证并发安全

type Key struct {
	Term  int
	Index int
}

type Log struct {
	K Key
	V string
}

type LogSet struct {
	logs         []Log
	committedKey Key
	m            sync.RWMutex
}

func (k Key) Greater(key Key) bool {
	if k.Term == key.Term {
		return k.Index > key.Index
	}
	return k.Term > key.Term
}

func (k Key) Less(key Key) bool {
	if k.Term == key.Term {
		return k.Index < key.Index
	}
	return k.Term < key.Term
}

func (k Key) Equals(key Key) bool {
	return k.Term == key.Term && k.Index == key.Index
}

func LogToString(content Log) string {
	return fmt.Sprintf("%d$%d^%s", content.K.Term, content.K.Index, content.V)
}

func StringToLog(v string) (content Log, err error) {
	err = errors.New("error: illegal log.(string)")
	res := strings.SplitN(v, "^", 2)
	if len(res) < 2 {
		return
	}
	logStr := res[1]
	res = strings.SplitN(res[0], "$", 2)
	if len(res) < 2 {
		return
	}
	term, err := strconv.Atoi(res[0])
	if err != nil {
		return
	}
	index, err := strconv.Atoi(res[1])
	if err != nil {
		return
	}
	return Log{Key{Term: term, Index: index}, logStr}, nil
}

func (l *LogSet) Init(committedKeyTerm int, committedKeyIndex int) {
	l.committedKey = Key{Term: committedKeyTerm, Index: committedKeyIndex}
}

func (l *LogSet) GetLast() Key {
	res := Key{Term: -1, Index: -1}
	l.m.RLock()
	if len(l.logs) >= 1 {
		res = l.logs[len(l.logs)-1].K
	}
	l.m.RUnlock()
	return res
}

func (l *LogSet) GetSecondLast() Key {
	res := Key{Term: -1, Index: -1}
	l.m.RLock()
	if len(l.logs) >= 2 {
		res = l.logs[len(l.logs)-2].K
	}
	l.m.RUnlock()
	return res
}

func (l *LogSet) GetCommitted() Key {
	return l.committedKey
}

func (l *LogSet) Append(content Log) { // 幂等的增加日志
	l.m.Lock()
	if len(l.logs) == 0 || l.logs[len(l.logs)-1].K.Less(content.K) {
		l.logs = append(l.logs, content)
	}
	l.m.Unlock()
}

func (l *LogSet) GetPrevious(key Key) (Key, error) { // 如果key不存在，返回-1-1
	l.m.RLock()
	res := Key{Term: -1, Index: -1}
	if l.Iterator(key) == -1 {
		l.m.RUnlock()
		return res, errors.New("wrong key")
	}
	left, right := 0, len(l.logs)-1
	for left < right {
		mid := (left + right + 1) / 2
		if !l.logs[mid].K.Less(key) {
			right = mid - 1
		} else {
			left = mid
		}
	}
	if l.logs[left].K.Less(key) {
		res = l.logs[left].K
	}
	l.m.RUnlock()
	return res, nil
}

func (l *LogSet) GetNext(key Key) (Key, error) { // 如果key不存在，返回-1-1
	l.m.RLock()
	res := Key{Term: -1, Index: -1}
	if key.Equals(Key{-1, -1}) && len(l.logs) > 0 {
		l.m.RUnlock()
		return l.logs[0].K, nil
	}
	if l.Iterator(key) == -1 {
		l.m.RUnlock()
		return res, errors.New("wrong key")
	}
	left, right := 0, len(l.logs)-1
	for left < right {
		mid := (left + right) / 2
		if l.logs[mid].K.Greater(key) {
			right = mid
		} else {
			left = mid + 1
		}
	}
	if l.logs[left].K.Greater(key) {
		res = l.logs[left].K
	}
	l.m.RUnlock()
	return res, nil
}

func (l *LogSet) GetVByK(key Key) (string, error) { // 通过Key寻找指定日志，找不到返回空
	var res string
	err := errors.New("error: can not find this log by key")
	l.m.RLock()
	left, right := 0, len(l.logs)-1
	for left <= right {
		mid := (left + right) / 2
		if l.logs[mid].K.Equals(key) {
			err = nil
			res = l.logs[mid].V
			break
		} else if l.logs[mid].K.Greater(key) {
			right = mid - 1
		} else {
			left = mid + 1
		}
	}
	l.m.RUnlock()
	if err == nil {
		return res, nil
	} else {
		return "", err
	}
}

func (l *LogSet) Commit(key Key) (previousCommitted Key) { // 提交所有小于等于key的日志，幂等的提交日志
	l.m.Lock()
	previousCommitted = l.committedKey
	left, right := 0, len(l.logs)-1
	for left < right {
		mid := (left + right + 1) / 2
		if l.logs[mid].K.Greater(key) {
			right = mid - 1
		} else {
			left = mid
		}
	}
	if !l.logs[left].K.Greater(key) && previousCommitted.Less(l.logs[left].K) {
		l.committedKey = l.logs[left].K
	}
	l.m.Unlock()
	return
}

func (l *LogSet) Remove(key Key) ([]Log, error) { // 删除日志直到自己的日志Key不大于key
	l.m.Lock()
	var ret []Log
	err := errors.New("error: remove committed log")
	left, right := 0, len(l.logs)-1
	for left < right {
		mid := (left + right + 1) / 2
		if l.logs[mid].K.Greater(key) {
			right = mid - 1
		} else {
			left = mid
		}
	}
	if !l.logs[left].K.Greater(key) {
		if l.committedKey.Greater(l.logs[left].K) {
			l.m.Unlock()
			return ret, err
		}
		ret = make([]Log, len(l.logs)-left-1)
		copy(l.logs[left+1:len(l.logs)], ret)
		l.logs = l.logs[0 : left+1]
	} else {
		if !l.committedKey.Equals(Key{-1, -1}) {
			l.m.Unlock()
			return ret, err
		}
		ret = l.logs
		l.logs = []Log{}
	}
	l.m.Unlock()
	return ret, nil
}

func (l *LogSet) Iterator(key Key) int { // 根据Key返回迭代器，没找到返回-1，线程不安全
	left, right := 0, len(l.logs)-1
	for left <= right {
		mid := (left + right) / 2
		if l.logs[mid].K.Equals(key) {
			return mid
		} else if l.logs[mid].K.Greater(key) {
			right = mid - 1
		} else {
			left = mid + 1
		}
	}
	return -1
}

func (l *LogSet) GetLogsByRange(begin Key, end Key) []Log { // 返回 [begin, end]闭区间内的所有日志信息
	l.m.RLock()
	beginIter, endIter := l.Iterator(begin), l.Iterator(end)
	if beginIter == -1 || endIter == -1 || beginIter > endIter {
		l.m.RUnlock()
		return []Log{}
	} else {
		tmp := make([]Log, endIter-beginIter+1)
		copy(tmp, l.logs[beginIter:endIter+1])
		l.m.RUnlock()
		return tmp
	}
}

func (l *LogSet) GetKsByRange(begin Key, end Key) []Key { // 返回 [begin, end]区间内的所有日志信息
	l.m.RLock()
	beginIter, endIter := l.Iterator(begin), l.Iterator(end)
	if beginIter == -1 || endIter == -1 || beginIter > endIter {
		l.m.RUnlock()
		return []Key{}
	} else {
		tmp := make([]Key, endIter-beginIter+1)
		for i := beginIter; i <= endIter; i++ {
			tmp[i-beginIter] = l.logs[i].K
		}
		l.m.RUnlock()
		return tmp
	}
}

func (l *LogSet) GetAll() []Log { // 线程不安全
	return l.logs
}

func (l *LogSet) ToString() string {
	l.m.RLock()
	res := fmt.Sprintf("==== logs ====\ncontents: %v\ncommittedKey: %v\n==== logs ====", l.logs, l.committedKey)
	l.m.RUnlock()
	return res
}
