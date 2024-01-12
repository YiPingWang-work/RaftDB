package KVDB

import (
	"fmt"
	"strings"
	"testing"
)

func TestKVDBClient_Parser(t *testing.T) {
	db := KVDBClient{}
	u, _ := db.Parser("write   ' hello tim   '    12222   ")
	fmt.Println(len(strings.Split(u, "'")))
}
