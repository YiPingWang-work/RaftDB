package Store

import (
	"log"
)

type Store struct {
	medium   Medium
	confPath string
	filePath string
}

type Medium interface {
	getMeta(confPath string, meta interface{}) error // 获取元数据/配置
	updateMeta(confPath string, meta string) error   // 更新元数据/配置
	loadFrom0(filePath string, logs *[]string) error // 加载所有日志到内存
	loadFromCommitted(filePath string) error         // 将从已提交的位置到最后的日志加载到内存，未实现
	appendLog(filePath string, content string) error // 在结尾处追加日志
	popLog(filePath string) error                    // 删除结尾处的日志，未实现
}

func (s *Store) Init(m Medium, confPath string, filePath string, meta interface{}, logs *[]string) {
	s.medium, s.confPath, s.filePath = m, confPath, filePath
	if err := s.medium.getMeta(confPath, meta); err != nil {
		log.Println(err)
	}
	if err := s.medium.loadFrom0(filePath, logs); err != nil {
		log.Println(err)
	}
}

func (s *Store) AppendLog(content string) error {
	return s.medium.appendLog(s.filePath, content)
}

func (s *Store) UpdateMeta(meta string) error {
	return s.medium.updateMeta(s.confPath, meta)
}
