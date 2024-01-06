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
		if x == "me" {
			fmt.Println(me.ToString())
		} else if x == "log" {
			fmt.Println(logs.ToString())
		} else {
			fmt.Println("use 'me' to get node info, use 'log' to get log info")
		}
	}
}
