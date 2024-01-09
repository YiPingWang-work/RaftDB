package KVDB

import "fmt"

type kvdb struct{}

func (k *kvdb) Process(in string) (out string, agree bool, err error) {
	fmt.Printf("============================== %s\n", in)
	return in, true, nil
}

func (k *kvdb) UndoProcess(in string) (out string, agree bool, err error) {
	fmt.Printf("============================== !!%s\n", in)
	return in, true, nil
}
