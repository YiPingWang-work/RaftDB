package Store

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

type CommonFile struct{}

func (c *CommonFile) getMeta(confPath string, meta interface{}) error {
	f, err := os.ReadFile(confPath)
	if err != nil {
		return err
	}
	return json.Unmarshal(f, meta)
}

func (c *CommonFile) updateMeta(confPath string, meta string) error {
	return os.WriteFile(confPath, []byte(meta), 0777)
}

func (c *CommonFile) loadFrom0(filePath string, logs *[]string) error {
	f, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}
	tmp := strings.Split(string(f), "\n")
	for _, v := range tmp {
		*logs = append(*logs, v)
	}
	return nil
}

func (c *CommonFile) loadFromCommitted(filePath string) error {
	return nil
}

func (c *CommonFile) appendLog(filePath string, content string) error {
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, 0777)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = fmt.Fprint(f, content)
	return err
}

func (c *CommonFile) popLog(filePath string) error {
	f, err := os.OpenFile(filePath, os.O_RDWR, 0777)
	if err != nil {
		return err
	}
	defer f.Close()
	return nil
}
