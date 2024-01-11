package Commenfile

import (
	"fmt"
	"os"
)

/*
type Medium interface {
	Init(mediumParam interface{}) error
	Read(path string, content *string) error
	Write(path string, content string) error
	Append(path string, content string) error
}
*/

type CommonFile struct{}

func (c *CommonFile) Init(interface{}) error {
	return nil
}

func (c *CommonFile) Read(path string, content *string) error {
	f, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	*content = string(f)
	return nil
}

func (c *CommonFile) Write(path string, content string) error {
	return os.WriteFile(path, []byte(content), 0777)
}

func (c *CommonFile) Append(path string, content string) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0777)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = fmt.Fprint(f, content)
	return err
}
