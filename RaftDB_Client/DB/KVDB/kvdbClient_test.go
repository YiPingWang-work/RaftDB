package KVDB

import (
	"fmt"
	"testing"
)

func TestKVDBClient_Parser(t *testing.T) {
	db := KVDBClient{}
	fmt.Println(db.Parser("read 'er er  "))
}
