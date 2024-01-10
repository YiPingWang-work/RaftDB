package Log

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

// 必须保证并发安全

type LogKey struct {
	Term  int
	Index int
}

type LogContent struct {
	Key LogKey
	Log string
}

type Logs struct {
	contents     []LogContent
	committedKey LogKey
	m            sync.RWMutex
}

func (k LogKey) Greater(key LogKey) bool {
	if k.Term == key.Term {
		return k.Index > key.Index
	}
	return k.Term > key.Term
}

func (k LogKey) Less(key LogKey) bool {
	if k.Term == key.Term {
		return k.Index < key.Index
	}
	return k.Term < key.Term
}

func (k LogKey) Equals(key LogKey) bool {
	return k.Term == key.Term && k.Index == key.Index
}

func LogToString(content LogContent) string {
	return fmt.Sprintf("%d$%d^%s\n", content.Key.Term, content.Key.Index, string(content.Log))
}

func StringToLog(v string) (content LogContent, err error) {
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
	return LogContent{LogKey{Term: term, Index: index}, logStr}, nil
}

func (l *Logs) Init(committedKeyTerm int, committedKeyIndex int) {
	l.committedKey = LogKey{Term: committedKeyTerm, Index: committedKeyIndex}
}

func (l *Logs) GetLast() LogKey {
	res := LogKey{Term: -1, Index: -1}
	l.m.RLock()
	if len(l.contents) >= 1 {
		res = l.contents[len(l.contents)-1].Key
	}
	l.m.RUnlock()
	return res
}

func (l *Logs) GetSecondLast() LogKey {
	res := LogKey{Term: -1, Index: -1}
	l.m.RLock()
	if len(l.contents) >= 2 {
		res = l.contents[len(l.contents)-2].Key
	}
	l.m.RUnlock()
	return res
}

func (l *Logs) GetCommitted() LogKey {
	return l.committedKey
}

func (l *Logs) Append(content LogContent) { // 幂等的增加日志
	l.m.Lock()
	if len(l.contents) == 0 || l.contents[len(l.contents)-1].Key.Less(content.Key) {
		l.contents = append(l.contents, content)
	}
	l.m.Unlock()
}

func (l *Logs) GetPrevious(key LogKey) LogKey { // 如果key不存在，返回-1-1
	l.m.RLock()
	res := LogKey{Term: -1, Index: -1}
	if l.Iterator(key) == -1 {
		l.m.RUnlock()
		return res
	}
	left, right := 0, len(l.contents)-1
	for left < right {
		mid := (left + right + 1) / 2
		if !l.contents[mid].Key.Less(key) {
			right = mid - 1
		} else {
			left = mid
		}
	}
	if l.contents[left].Key.Less(key) {
		res = l.contents[left].Key
	}
	l.m.RUnlock()
	return res
}

func (l *Logs) GetNext(key LogKey) LogKey { // 如果key不存在，返回-1-1
	l.m.RLock()
	res := LogKey{Term: -1, Index: -1}
	if key.Equals(LogKey{-1, -1}) && len(l.contents) > 0 {
		l.m.RUnlock()
		return l.contents[0].Key
	}
	if l.Iterator(key) == -1 {
		l.m.RUnlock()
		return res
	}
	left, right := 0, len(l.contents)-1
	for left < right {
		mid := (left + right) / 2
		if l.contents[mid].Key.Greater(key) {
			right = mid
		} else {
			left = mid + 1
		}
	}
	if l.contents[left].Key.Greater(key) {
		res = l.contents[left].Key
	}
	l.m.RUnlock()
	return res
}

func (l *Logs) GetContentByKey(key LogKey) (string, error) { // 通过Key寻找指定日志，找不到返回空
	var res string
	err := errors.New("error: can not find this log by key")
	l.m.RLock()
	left, right := 0, len(l.contents)-1
	for left <= right {
		mid := (left + right) / 2
		if l.contents[mid].Key.Equals(key) {
			err = nil
			res = l.contents[mid].Log
			break
		} else if l.contents[mid].Key.Greater(key) {
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

func (l *Logs) Commit(key LogKey) (previousCommitted LogKey) { // 提交所有小于等于key的日志，幂等的提交日志
	l.m.Lock()
	previousCommitted = l.committedKey
	left, right := 0, len(l.contents)-1
	for left < right {
		mid := (left + right + 1) / 2
		if l.contents[mid].Key.Greater(key) {
			right = mid - 1
		} else {
			left = mid
		}
	}
	if !l.contents[left].Key.Greater(key) && previousCommitted.Less(l.contents[left].Key) {
		l.committedKey = l.contents[left].Key
	}
	l.m.Unlock()
	return
}

func (l *Logs) Remove(key LogKey) ([]LogContent, error) { // 删除日志直到自己的日志Key不大于key
	l.m.Lock()
	var ret []LogContent
	err := errors.New("error: remove committed log")
	left, right := 0, len(l.contents)-1
	for left < right {
		mid := (left + right + 1) / 2
		if l.contents[mid].Key.Greater(key) {
			right = mid - 1
		} else {
			left = mid
		}
	}
	if !l.contents[left].Key.Greater(key) {
		if l.committedKey.Greater(l.contents[left].Key) {
			l.m.Unlock()
			return ret, err
		}
		ret = make([]LogContent, len(l.contents)-left-1)
		copy(l.contents[left+1:len(l.contents)], ret)
		l.contents = l.contents[0 : left+1]
	} else {
		if !l.committedKey.Equals(LogKey{-1, -1}) {
			l.m.Unlock()
			return ret, err
		}
		ret = l.contents
		l.contents = []LogContent{}
	}
	l.m.Unlock()
	return ret, nil
}

func (l *Logs) Iterator(key LogKey) int { // 根据Key返回迭代器，没找到返回-1，线程不安全
	left, right := 0, len(l.contents)-1
	for left <= right {
		mid := (left + right) / 2
		if l.contents[mid].Key.Equals(key) {
			return mid
		} else if l.contents[mid].Key.Greater(key) {
			right = mid - 1
		} else {
			left = mid + 1
		}
	}
	return -1
}

func (l *Logs) GetLogsByRange(begin LogKey, end LogKey) []LogContent { // 返回 [begin, end]闭区间内的所有日志信息
	l.m.RLock()
	beginIter, endIter := l.Iterator(begin), l.Iterator(end)
	if beginIter == -1 || endIter == -1 || beginIter > endIter {
		l.m.RUnlock()
		return []LogContent{}
	} else {
		tmp := make([]LogContent, endIter-beginIter+1)
		copy(tmp, l.contents[beginIter:endIter+1])
		l.m.RUnlock()
		return tmp
	}
}

func (l *Logs) GetKeysByRange(begin LogKey, end LogKey) []LogKey { // 返回 [begin, end]区间内的所有日志信息
	l.m.RLock()
	beginIter, endIter := l.Iterator(begin), l.Iterator(end)
	if beginIter == -1 || endIter == -1 || beginIter > endIter {
		l.m.RUnlock()
		return []LogKey{}
	} else {
		tmp := make([]LogKey, endIter-beginIter+1)
		for i := beginIter; i <= endIter; i++ {
			tmp[i-beginIter] = l.contents[i].Key
		}
		l.m.RUnlock()
		return tmp
	}
}

func (l *Logs) ToString() string {
	l.m.RLock()
	res := fmt.Sprintf("==== logs ====\ncontents: %v\ncommittedKey: %v\n==== logs ====", l.contents, l.committedKey)
	l.m.RUnlock()
	return res
}
