package main

import (
	"RaftDB/Bottom"
	"RaftDB/Log"
	"RaftDB/Logic"
	"RaftDB/Meta"
	"RaftDB/Monitor"
	"RaftDB/Order"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"time"
)

type MemberType int

func gogo(confPath string, filePath string) {
	var bottom Bottom.Bottom                                                 // 声明通信和存储底座，内部数据结构线程不安全
	var meta Meta.Meta                                                       // 新建元数据，元数据线程不安全
	var logs Log.Logs                                                        // 新建日志文件，日志文件线程安全
	var me Logic.Me                                                          // 新建Raft层，内部数据结构线程不安全
	ToLogicChan := make(chan Order.Order, 100000)                            // 创建通讯管道，管道线程安全
	ToBottomChan := make(chan Order.Order, 100000)                           // 创建通讯管道，管道线程安全
	bottom.Init(confPath, filePath, &meta, &logs, ToBottomChan, ToLogicChan) // 初始化系统底座，初始化meta和logs(传出参数)
	rand.Seed(time.Now().UnixNano() + int64(meta.Id)%1024)                   // 设置随机因子
	log.Printf("\n%s\n", meta.ToString())                                    // 输出元数据信息
	log.Printf("\n%s\n", logs.ToString())                                    // 输出日志信息
	me.Init(&meta, &logs, ToLogicChan, ToBottomChan)                         // 初始化Raft层，raft层和bottom可以共享访问log，但是meta只有Raft层可以访问
	go bottom.Run()                                                          // 运行底座，运行网络监听，开始对接端口Msg.ToLogicChan, Order.ReplyChan监听
	go Monitor.Monitor(&me, &logs, &bottom)                                  // 开启监控程序，三个参数均是只读参数，其中meta会永久的保留在me中，通过me唯一访问
	me.Run()                                                                 // 运行逻辑层，开始对接端口Msg.ToLogicChan, Order.ReplyChan监听
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
