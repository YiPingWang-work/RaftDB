package KVDB

type kvdb struct{}

func (k *kvdb) Process(in string) (out string, err error) {
	return in, nil
}
