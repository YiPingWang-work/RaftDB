package main

import (
	"RaftDB/Custom/Communicate/RPC"
	"RaftDB/Custom/DB/KVDB"
	"RaftDB/Custom/Store/Commenfile"
	"RaftDB/Kernel/Bottom"
	"RaftDB/Kernel/Crown"
	"RaftDB/Kernel/Log"
	"RaftDB/Kernel/Logic"
	"RaftDB/Kernel/Meta"
	"RaftDB/Kernel/Pipe/Order"
	"RaftDB/Kernel/Pipe/Something"
	"RaftDB/Monitor"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"time"
)

// 你可以修改Gogo函数使其完成你的定制化功能

func Gogo(confPath string, logPath string, medium Bottom.Medium, cable Bottom.Cable, app Crown.App) {
	var bottom Bottom.Bottom                               // 声明通信和存储底座，内部数据结构线程不安全
	var meta Meta.Meta                                     // 新建元数据，元数据线程不安全
	var logSet Log.LogSet                                  // 新建日志文件，日志文件线程安全
	var me Logic.Me                                        // 新建Raft层，内部数据结构线程不安全
	var crown Crown.Crown                                  // 新建上层应用服务
	fromBottomChan := make(chan Order.Order, 10000)        // 创建下层通讯管道，管道线程安全
	toBottomChan := make(chan Order.Order, 10000)          // 创建下层通讯管道，管道线程安全
	toCrownChan := make(chan Something.Something, 10000)   // 创建上层管道
	fromCrownChan := make(chan Something.Something, 10000) // 创建上层通讯管道
	bottom.Init(confPath, logPath, &meta, &logSet, medium, cable,
		toBottomChan, fromBottomChan, nil, fromBottomChan) // 初始化系统底座，初始化meta和logs（传入传出参数）
	rand.Seed(time.Now().UnixNano() + int64(meta.Id)%1024)                            // 设置随机因子
	log.Printf("\n%s\n", meta.ToString())                                             // 输出元数据信息
	log.Printf("\n%s\n", logSet.ToString())                                           // 输出日志信息
	me.Init(&meta, &logSet, fromBottomChan, toBottomChan, fromCrownChan, toCrownChan) // 初始化Raft层，raft层和bottom可以共享访问log，但是meta只有Raft层可以访问
	crown.Init(&logSet, app, toCrownChan, fromCrownChan)
	go bottom.Run() // 运行底座，运行网络监听，开始对接端口Msg.ToLogicChan, Order.ReplyChan监听
	go crown.Run()
	go me.Run()
	Monitor.Monitor(&me, &logSet, &bottom, &crown)
}

func main() {
	confPath, err := filepath.Abs(os.Args[1])
	if err != nil {
		log.Println("input error: illegal file")
		return
	}
	filePath, err := filepath.Abs(os.Args[2])
	if err != nil {
		log.Println("input error: illegal file")
		return
	}
	log.Printf("config: %s, datafile: %s\n", confPath, filePath)
	Gogo(confPath, filePath, &Commenfile.CommonFile{}, &RPC.RPC{}, &KVDB.KVDB{}) // 原神，启动！

}
