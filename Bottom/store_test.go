package Bottom

import (
	"RaftDB/Bottom/Store"
	"RaftDB/Log"
	"RaftDB/Meta"
	"log"
	"testing"
)

func TestStore(t *testing.T) {
	meta := Meta.Meta{}
	logs := Log.Logs{}
	logs.Init(-1, -1)
	var tmp []string
	store := Store.Store{}
	store.Init(new(Store.CommonFile), "../Meta/raftdb.conf", "../DATA/data.aof", &meta, &tmp)
	log.Println(meta)
	for _, v := range tmp {
		if res, err := Log.StringToLog(v); err == nil {
			logs.Append(res)
		}
	}
	log.Println(logs)
	logs.Append(Log.Content{LogKey: Log.LogKeyType{Index: 1, Term: 0}, Log: Log.LogType("10")})
	logs.Append(Log.Content{LogKey: Log.LogKeyType{Index: 1, Term: 1}, Log: Log.LogType("11")})
	logs.Append(Log.Content{LogKey: Log.LogKeyType{Index: 1, Term: 2}, Log: Log.LogType("21")})
	logs.Append(Log.Content{LogKey: Log.LogKeyType{Index: 1, Term: 0}, Log: Log.LogType("10i")})
	log.Println(logs)
	for _, v := range logs.GetLogsByRange(Log.LogKeyType{Index: 0, Term: 0}, Log.LogKeyType{Index: 1, Term: 2}) {
		log.Println(v)
		if err := store.AppendLog(Log.LogToString(v)); err != nil {
			log.Println(err)
		}
	}
}
