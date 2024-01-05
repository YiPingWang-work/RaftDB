package Monitor

import (
	"RaftDB/Log"
	"RaftDB/Logic"
	"fmt"
)

func Monitor(me *Logic.Me, logs *Log.Logs) { // 禁止修改参数
	for {
		var x string
		fmt.Scanln(&x)
		if x == "state" {
			fmt.Println(me.ToString())
		} else if x == "log" {
			fmt.Println(logs.ToString())
		} else {
			fmt.Println("use state or log")
		}
	}
}
