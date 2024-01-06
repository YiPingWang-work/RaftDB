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
	var bottom Bottom.Bottom                                                 // 声明通信和存储底座
	var meta Meta.Meta                                                       // 新建元数据
	var logs Log.Logs                                                        // 新建日志文件
	var me Logic.Me                                                          // 新建Raft层
	ToLogicChan := make(chan Order.Order, 1000)                              // 创建通讯管道
	ToBottomChan := make(chan Order.Order, 1000)                             // 创建通讯管道
	bottom.Init(confPath, filePath, &meta, &logs, ToBottomChan, ToLogicChan) // 初始化系统底座，初始化meta和logs
	rand.Seed(time.Now().UnixNano() + int64(meta.Id)%1024)                   // 设置随机因子
	log.Printf("\n%s\n", meta.ToString())                                    // 输出元数据信息
	log.Printf("\n%s\n", logs.ToString())                                    // 输出日志信息
	me.Init(&meta, &logs, ToLogicChan, ToBottomChan)                         // 初始化Raft层
	go bottom.Run()                                                          // 运行底座，运行网络监听，开始对接端口Msg.ToLogicChan, Order.ReplyChan监听
	go Monitor.Monitor(&me, &logs, &bottom)                                  // 开启监控程序
	me.Run()                                                                 // 运行逻辑层，开始对接端口Msg.ToLogicChan, Order.ReplyChan监听
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
