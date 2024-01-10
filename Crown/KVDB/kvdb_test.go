package KVDB

import (
	"fmt"
	"testing"
)

func TestKvdb(t *testing.T) {
	var x kvdb
	x.Init()
	fmt.Println(x.Process("read hello"))
	fmt.Println(x.Process("write hello world"))
	fmt.Println(x.Process("read hello"))
}
