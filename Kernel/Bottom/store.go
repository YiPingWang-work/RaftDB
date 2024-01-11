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

/*
存储介质接口需要实现初始化，读写和追加写功能
*/

type Medium interface {
	Init(mediumParam interface{}) error
	Read(path string, content *string) error
	Write(path string, content string) error
	Append(path string, content string) error
}

/*
存储系统初始化，需要初始化存储介质，之后通过该介质读取磁盘中的配置信息和日志，并将其应用到日志系统和元数据系统。
*/

func (s *Store) initAndLoad(confPath string, filePath string, meta *Meta.Meta, logs *Log.LogSet,
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

func (s *Store) appendLogs(logs *[]Log.Log) error {
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

func (s *Store) loadFrom0(logPath string, logs *Log.LogSet) error {
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
