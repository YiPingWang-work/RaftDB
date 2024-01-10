package Bottom

import (
	"RaftDB/Kernel/Log"
	"RaftDB/Kernel/Meta"
	"encoding/json"
	"log"
	"strings"
)

type Store struct {
	medium   Medium
	confPath string
	filePath string
}

type Medium interface {
	Init(mediumParam interface{}) error
	Read(path string, content *string) error
	Write(path string, content string) error
	Append(path string, content string) error
}

func (s *Store) initAndLoad(confPath string, filePath string, meta *Meta.Meta, logs *Log.Logs,
	m Medium,
	mediumParam interface{}) error {

	s.medium, s.confPath, s.filePath = m, confPath, filePath
	if err := s.medium.Init(mediumParam); err != nil {
		return err
	}
	if err := s.getMeta(confPath, meta); err != nil {
		return err
	}
	if err := s.loadFrom0(filePath, logs); err != nil {
		return err
	}
	return nil
}

func (s *Store) appendLogs(logs *[]Log.LogContent) error {
	for _, v := range *logs {
		if err := s.medium.Append(s.filePath, Log.LogToString(v)); err != nil {
			log.Println(err)
		}
	}
	return nil
}

func (s *Store) updateMeta(meta string) error {
	return s.medium.Write(s.confPath, meta)
}

func (s *Store) getMeta(metaPath string, meta *Meta.Meta) error {
	var str string
	if err := s.medium.Read(metaPath, &str); err != nil {
		return err
	}
	return json.Unmarshal([]byte(str), meta)
}

func (s *Store) loadFrom0(logPath string, logs *Log.Logs) error {
	var str string
	if err := s.medium.Read(logPath, &str); err != nil {
		return err
	}
	tmp := strings.Split(str, "\n")
	for _, v := range tmp {
		if res, err := Log.StringToLog(v); err == nil {
			logs.Append(res)
		}
	}
	return nil
}
