package Log

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

// 必须保证并发安全

type LogType string

type LogKeyType struct {
	Term  int
	Index int
}

type Content struct {
	LogKey LogKeyType
	Log    LogType
}

type Logs struct {
	contents     []Content
	committedKey LogKeyType
	m            sync.RWMutex
}

func (k LogKeyType) Greater(key LogKeyType) bool {
	if k.Term == key.Term {
		return k.Index > key.Index
	}
	return k.Term > key.Term
}

func (k LogKeyType) Less(key LogKeyType) bool {
	if k.Term == key.Term {
		return k.Index < key.Index
	}
	return k.Term < key.Term
}

func (k LogKeyType) Equals(key LogKeyType) bool {
	return k.Term == key.Term && k.Index == key.Index
}

func LogToString(content Content) string {
	return fmt.Sprintf("%d$%d^%s\n", content.LogKey.Term, content.LogKey.Index, string(content.Log))
}

func StringToLog(v string) (content Content, err error) {
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
	return Content{LogKeyType{Term: term, Index: index}, LogType(logStr)}, nil
}

func (l *Logs) Init(committedKeyTerm int, committedKeyIndex int) {
	l.committedKey = LogKeyType{Term: committedKeyTerm, Index: committedKeyIndex}
}

func (l *Logs) GetLast() LogKeyType { // 获取最后一笔日志Key
	res := LogKeyType{Term: -1, Index: -1}
	l.m.RLock()
	if len(l.contents) >= 1 {
		res = l.contents[len(l.contents)-1].LogKey
	}
	l.m.RUnlock()
	return res
}

func (l *Logs) GetSecondLast() LogKeyType { // 获取倒数第二笔日志Key
	res := LogKeyType{Term: -1, Index: -1}
	l.m.RLock()
	if len(l.contents) >= 2 {
		res = l.contents[len(l.contents)-2].LogKey
	}
	l.m.RUnlock()
	return res
}

func (l *Logs) GetCommitted() LogKeyType { // 获取最后一笔提交日志Key
	return l.committedKey
}

func (l *Logs) Append(content Content) { // 幂等的增加日志
	l.m.Lock()
	if len(l.contents) == 0 || l.contents[len(l.contents)-1].LogKey.Less(content.LogKey) {
		l.contents = append(l.contents, content)
	}
	l.m.Unlock()
}

//func (l *Logs) GetContentByKeySlow(key LogKeyType) (LogType, error) { // 通过Key寻找指定日志，找不到返回空
//	for i := 0; i < len(l.contents); i++ {
//		if l.contents[i].LogKey.Equals(key) {
//			return l.contents[i].Log, nil
//		}
//	}
//	return LogType(""), errors.New("error: can not find this log by key")
//}
//
//func (l *Logs) CommitSlow(key LogKeyType) (previousCommitted LogKeyType) { // 提交所有小于等于key的日志，幂等的提交日志
//	previousCommitted = l.committedKey
//	for i := len(l.contents) - 1; i >= 0; i-- {
//		if !l.contents[i].LogKey.Greater(key) {
//			if previousCommitted.Less(l.contents[i].LogKey) {
//				l.committedKey = l.contents[i].LogKey
//			}
//			break
//		}
//	}
//	return
//}
//
//func (l *Logs) RemoveSlow(key LogKeyType) (removeLogKey LogKeyType, err error) { // 删除日志直到自己的日志Key不大于key
//	removeLogKey, err = LogKeyType{Term: -1, Index: -1}, nil
//	for i := len(l.contents) - 1; i >= 0; i-- {
//		if !l.contents[i].LogKey.Greater(key) {
//			if l.committedKey.Less(l.contents[i].LogKey) {
//				removeLogKey = l.contents[i].LogKey
//			} else {
//				err = errors.New("error: remove committed log")
//			}
//			break
//		}
//	}
//	if err == nil {
//		for len(l.contents) >= 0 && !l.contents[len(l.contents)-1].LogKey.Equals(removeLogKey) {
//			l.contents = l.contents[:len(l.contents)-1]
//		}
//	}
//	return
//}
//
//func (l *Logs) GetPreviousSlow(key LogKeyType) LogKeyType {
//	for i := len(l.contents) - 1; i >= 1; i-- {
//		if l.contents[i].LogKey.Equals(key) {
//			return l.contents[i-1].LogKey
//		}
//	}
//	return LogKeyType{Term: -1, Index: -1}
//}
//
//func (l *Logs) GetNextSlow(key LogKeyType) LogKeyType {
//	if key.Term == -1 && len(l.contents) > 0 {
//		return l.contents[0].LogKey
//	}
//	for i := 0; i < len(l.contents)-1; i++ {
//		if l.contents[i].LogKey.Equals(key) {
//			return l.contents[i+1].LogKey
//		}
//	}
//	return LogKeyType{Term: -1, Index: -1}
//}
//
//func (l *Logs) GetLogsByRangeSlow(begin LogKeyType, end LogKeyType) []Content {
//	l.m.RLock()
//	fl, contents := false, []Content{}
//	for i := 0; i < len(l.contents); i++ {
//		if l.contents[i].LogKey.Equals(begin) {
//			fl = true
//		}
//		if fl {
//			contents = append(contents, l.contents[i])
//		}
//		if l.contents[i].LogKey.Equals(end) {
//			fl = false
//		}
//	}
//	l.m.RUnlock()
//	return contents
//}

func (l *Logs) GetPrevious(key LogKeyType) LogKeyType {
	res := LogKeyType{Term: -1, Index: -1}
	l.m.RLock()
	left, right := 0, len(l.contents)
	for left <= right {
		mid := (left + right) / 2
		if l.contents[mid].LogKey.Equals(key) {
			if mid >= 1 {
				res = l.contents[mid-1].LogKey
			}
			break
		} else if l.contents[mid].LogKey.Greater(key) {
			right = mid - 1
		} else {
			left = mid + 1
		}
	}
	l.m.RUnlock()
	return res
}

func (l *Logs) GetNext(key LogKeyType) LogKeyType {
	l.m.RLock()
	left, right := 0, len(l.contents)-1
	for left < right {
		mid := (left + right) / 2
		if l.contents[mid].LogKey.Greater(key) {
			right = mid
		} else {
			left = mid + 1
		}
	}
	res := LogKeyType{Term: -1, Index: -1}
	if l.contents[left].LogKey.Greater(key) {
		res = l.contents[left].LogKey
	}
	l.m.RUnlock()
	return res
}

func (l *Logs) GetContentByKey(key LogKeyType) (LogType, error) { // 通过Key寻找指定日志，找不到返回空
	var res LogType
	err := errors.New("error: can not find this log by key")
	l.m.RLock()
	left, right := 0, len(l.contents)
	for left <= right {
		mid := (left + right) / 2
		if l.contents[mid].LogKey.Equals(key) {
			err = nil
			res = l.contents[mid].Log
			break
		} else if l.contents[mid].LogKey.Greater(key) {
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

func (l *Logs) Commit(key LogKeyType) (previousCommitted LogKeyType) { // 提交所有小于等于key的日志，幂等的提交日志
	l.m.RLock()
	previousCommitted = l.committedKey
	left, right := 0, len(l.contents)-1
	for left < right {
		mid := (left + right + 1) / 2
		if l.contents[mid].LogKey.Greater(key) {
			right = mid - 1
		} else {
			left = mid
		}
	}
	if !l.contents[left].LogKey.Greater(key) && previousCommitted.Less(l.contents[left].LogKey) {
		l.committedKey = l.contents[left].LogKey
	}
	l.m.RUnlock()
	return
}

func (l *Logs) Remove(key LogKeyType) (LogKeyType, error) { // 删除日志直到自己的日志Key不大于key
	l.m.Lock()
	errRes, err := LogKeyType{-1, -1}, errors.New("error: remove committed log")
	left, right := 0, len(l.contents)-1
	for left < right {
		mid := (left + right + 1) / 2
		if l.contents[mid].LogKey.Greater(key) {
			right = mid - 1
		} else {
			left = mid
		}
	}
	if !l.contents[left].LogKey.Greater(key) {
		if l.committedKey.Greater(l.contents[left].LogKey) {
			l.m.Unlock()
			return errRes, err
		}
		l.contents = l.contents[0 : left+1]
	} else {
		if !l.committedKey.Equals(LogKeyType{-1, -1}) {
			l.m.Unlock()
			return errRes, err
		}
		l.contents = []Content{}
	}
	l.m.Unlock()
	return l.GetLast(), nil
}

func (l *Logs) Iterator(key LogKeyType) int { // 根据Key返回迭代器，没找到返回-1，线程不安全
	left, right := 0, len(l.contents)
	for left <= right {
		mid := (left + right) / 2
		if l.contents[mid].LogKey.Equals(key) {
			return mid
		} else if l.contents[mid].LogKey.Greater(key) {
			right = mid - 1
		} else {
			left = mid + 1
		}
	}
	return -1
}

func (l *Logs) GetLogsByRange(begin LogKeyType, end LogKeyType) []Content { // 返回 [begin, end]区间内的所有日志信息
	l.m.RLock()
	beginIter, endIter := l.Iterator(begin), l.Iterator(end)
	if beginIter == -1 || endIter == -1 || beginIter > endIter {
		l.m.RUnlock()
		return []Content{}
	} else {
		tmp := make([]Content, endIter-beginIter+1)
		copy(tmp, l.contents[beginIter:endIter+1])
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
