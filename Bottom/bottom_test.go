package Bottom

import (
	"RaftDB/Bottom/Store"
	"RaftDB/Log"
	"RaftDB/Meta"
	"log"
	"testing"
)

func TestBottom(t *testing.T) {
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
	logs.Append(Log.LogContent{Key: Log.LogKey{Index: 1, Term: 0}, Log: "10"})
	logs.Append(Log.LogContent{Key: Log.LogKey{Index: 1, Term: 1}, Log: "11"})
	logs.Append(Log.LogContent{Key: Log.LogKey{Index: 1, Term: 2}, Log: "21"})
	logs.Append(Log.LogContent{Key: Log.LogKey{Index: 1, Term: 0}, Log: "10i"})
	log.Println(logs)
	for _, v := range logs.GetLogsByRange(Log.LogKey{Index: 0, Term: 0}, Log.LogKey{Index: 1, Term: 2}) {
		log.Println(v)
		if err := store.AppendLog(Log.LogToString(v)); err != nil {
			log.Println(err)
		}
	}
}
