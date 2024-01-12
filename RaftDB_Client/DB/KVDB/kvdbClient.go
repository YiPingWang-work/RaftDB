package KVDB

import (
	"strings"
)

type KVDBClient struct{}

func (k *KVDBClient) Parser(order string) (string, bool) {
	if strings.Count(order, "'")%2 == 1 {
		return "", false
	}
	var res []string
	tmp := strings.Split(order, "'")
	for _, v := range tmp {
		v := strings.TrimSpace(v)
		if len(v) > 0 {
			res = append(res, v)
		}
	}
Again:
	if len(res) == 3 && res[0] == "write" {
		return "write'" + res[1] + "'" + res[2], true
	} else if len(res) == 2 {
		if res[0] == "watch" {
			return "watch'write'" + res[1], true
		} else if res[0] == "read" {
			return "read'" + res[1], true
		}
		var res2 []string
		tmp := strings.Split(res[0], " ")
		for _, v := range tmp {
			v := strings.TrimSpace(v)
			if len(v) > 0 {
				res2 = append(res2, v)
			}
		}
		if len(res2) == 2 && res2[0] == "write" {
			return "write'" + res2[1] + "'" + res[1], true
		}
	} else if len(res) == 1 {
		var res2 []string
		tmp := strings.Split(res[0], " ")
		for _, v := range tmp {
			v := strings.TrimSpace(v)
			if len(v) > 0 {
				res2 = append(res2, v)
			}
		}
		res = res2
		if len(res) > 1 {
			goto Again
		}
	}
	return "", false
}
