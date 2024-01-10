package main

import (
	"RaftDB/Custom/Communicate/RPC"
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

type MemberType int

func gogo(confPath string, filePath string) {
	var bottom Bottom.Bottom                               // 声明通信和存储底座，内部数据结构线程不安全
	var meta Meta.Meta                                     // 新建元数据，元数据线程不安全
	var logs Log.LogSet                                    // 新建日志文件，日志文件线程安全
	var me Logic.Me                                        // 新建Raft层，内部数据结构线程不安全
	var crown Crown.Crown                                  // 新建上层应用服务
	fromBottomChan := make(chan Order.Order, 10000)        // 创建下层通讯管道，管道线程安全
	toBottomChan := make(chan Order.Order, 10000)          // 创建下层通讯管道，管道线程安全
	toCrownChan := make(chan Something.Something, 10000)   // 创建上层管道
	fromCrownChan := make(chan Something.Something, 10000) // 创建上层通讯管道
	bottom.Init(confPath, filePath, &meta, &logs,
		&Commenfile.CommonFile{}, &RPC.RPC{}, toBottomChan, fromBottomChan,
		nil, fromBottomChan) // 初始化系统底座，初始化meta和logs(传出参数)
	rand.Seed(time.Now().UnixNano() + int64(meta.Id)%1024)                          // 设置随机因子
	log.Printf("\n%s\n", meta.ToString())                                           // 输出元数据信息
	log.Printf("\n%s\n", logs.ToString())                                           // 输出日志信息
	me.Init(&meta, &logs, fromBottomChan, toBottomChan, fromCrownChan, toCrownChan) // 初始化Raft层，raft层和bottom可以共享访问log，但是meta只有Raft层可以访问
	crown.Init(toCrownChan, fromCrownChan)                                          // 初始化上层
	go bottom.Run()                                                                 // 运行底座，运行网络监听，开始对接端口Msg.ToLogicChan, Order.ReplyChan监听
	go crown.Run()                                                                  // 上层启动
	go Monitor.Monitor(&me, &logs, &bottom, &crown)                                 // 开启监控程序，三个参数均是只读参数，其中meta会永久的保留在me中，通过me唯一访问
	me.Run()                                                                        // 运行逻辑层，开始对接端口Msg.ToLogicChan, Order.ReplyChan监听
	// Monitor和me可能会线程不安全，但是Monitor作为用户接口调试使用，正常运行时关闭
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
	gogo(confPath, filePath) // 原神，启动！
}
