package Log

import (
	"log"
	"testing"
)

func TestLogs(t *testing.T) {
	logs := Logs{}
	logs.Init(-1, -1)
	logs.Append(Content{LogKeyType{0, 1}, LogType("01")})
	logs.Append(Content{LogKeyType{0, 2}, LogType("02")})
	logs.Append(Content{LogKeyType{0, 3}, LogType("03")})
	logs.Append(Content{LogKeyType{2, 1}, LogType("21")})
	logs.Append(Content{LogKeyType{2, 2}, LogType("22")})
	logs.Append(Content{LogKeyType{2, 1}, LogType("2uuu}")})
	logs.Append(Content{LogKeyType{4, 1}, LogType("2uuu}")})
	//log.Printf(logs.ToString())
	//logs.Remove(LogKeyType{2, 2})
	log.Printf(logs.ToString())
	log.Println(logs.GetPrevious(LogKeyType{4, 3}))
	log.Println(logs.Remove(LogKeyType{4, 3}))
	log.Printf(logs.ToString())
	log.Println(logs.Remove(LogKeyType{0, 1}))
	log.Printf(logs.ToString())
	log.Println(logs.Remove(LogKeyType{-1, -1}))
	//logs.Append(Content{LogKeyType{3, 2}, LogType("32")})
	//logs.Append(Content{LogKeyType{3, 4}, LogType("34")})
	//fmt.Println(logs.GetPrevious(LogKeyType{0, 3}))
	//fmt.Println(logs.GetPreviousSlow(LogKeyType{0, 3}))
	//fmt.Println(logs.GetPrevious(LogKeyType{3, 4}))
	//fmt.Println(logs.GetContentByKey(LogKeyType{3, 4}))
	//fmt.Println(logs.GetContentByKeySlow(LogKeyType{3, 4}))
	//fmt.Println(logs.Commit(LogKeyType{2, 3}))
	//fmt.Println(logs.Remove(LogKeyType{3, 2}))
	//fmt.Println(logs.Commit(LogKeyType{10, 10}))
	//fmt.Println(logs.Iterator(LogKeyType{2, 12}))
	//fmt.Println(logs.contents[logs.Iterator(LogKeyType{2, 1})])
	//fmt.Println(logs.GetLogsByRange(LogKeyType{0, 2}, LogKeyType{3, 2}))
	//fmt.Println(logs.GetNext(LogKeyType{-1, -1}))
	//fmt.Println(logs.GetNext(LogKeyType{-1, -1}))
	//fmt.Println(logs.GetNext(LogKeyType{2, 1}))
	//fmt.Println(logs.GetLogsByRange(LogKeyType{-1, -1}, LogKeyType{2, 1}))
	//fmt.Println(u.ToString())

}
