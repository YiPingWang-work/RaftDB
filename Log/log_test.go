package Log

import (
	"fmt"
	"log"
	"testing"
)

func TestLogs(t *testing.T) {
	logs := Logs{}
	logs.Init(-1, -1)
	logs.Append(LogContent{LogKey{0, 1}, LogType("01")})
	logs.Append(LogContent{LogKey{0, 2}, LogType("02")})
	logs.Append(LogContent{LogKey{0, 3}, LogType("03")})
	logs.Append(LogContent{LogKey{2, 1}, LogType("21")})
	logs.Append(LogContent{LogKey{2, 2}, LogType("22")})
	logs.Append(LogContent{LogKey{2, 1}, LogType("2uuu}")})
	logs.Append(LogContent{LogKey{4, 1}, LogType("2uuu}")})
	log.Println(logs.GetKeysByRange(LogKey{0, 2}, LogKey{4, 1}))
	logs.GetNext(LogKey{0, 1})
	////log.Printf(logs.ToString())
	////logs.Remove(LogKey{2, 2})
	//log.Printf(logs.ToString())
	//log.Println(logs.GetPrevious(LogKey{4, 3}))
	//log.Println(logs.Remove(LogKey{4, 3}))
	//log.Printf(logs.ToString())
	//log.Println(logs.Remove(LogKey{0, 1}))
	//log.Printf(logs.ToString())
	//log.Println(logs.Remove(LogKey{-1, -1}))
	//logs.Append(LogContent{LogKey{3, 2}, LogType("32")})
	//logs.Append(LogContent{LogKey{3, 4}, LogType("34")})
	//fmt.Println(logs.GetPrevious(LogKey{0, 3}))
	//fmt.Println(logs.GetPreviousSlow(LogKey{0, 3}))
	//fmt.Println(logs.GetPrevious(LogKey{3, 4}))
	//fmt.Println(logs.GetContentByKey(LogKey{3, 4}))
	//fmt.Println(logs.GetContentByKeySlow(LogKey{3, 4}))
	//fmt.Println(logs.Commit(LogKey{2, 3}))
	//fmt.Println(logs.Remove(LogKey{3, 2}))
	//fmt.Println(logs.Commit(LogKey{10, 10}))
	//fmt.Println(logs.Iterator(LogKey{2, 12}))
	//fmt.Println(logs.contents[logs.Iterator(LogKey{2, 1})])
	//fmt.Println(logs.GetLogsByRange(LogKey{0, 2}, LogKey{3, 2}))
	//fmt.Println(logs.GetNext(LogKey{-1, -1}))
	//fmt.Println(logs.GetNext(LogKey{-1, -1}))
	//fmt.Println(logs.GetNext(LogKey{2, 1}))
	//fmt.Println(logs.GetLogsByRange(LogKey{-1, -1}, LogKey{2, 1}))
	//fmt.Println(u.ToString())
	var x int
	fmt.Scanln(&x)
	fmt.Println(x)
}
