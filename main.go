package main

import (
	"flag"
	"time"
	"github.com/sirupsen/logrus"
	"os"
	"fmt"
)

var log = logrus.New()

func init() {
	log.Out = os.Stdout
	log.SetLevel(logrus.DebugLevel)
}

func main() {
	//Todo 获取参数.
	logFilePath := flag.String("logFilePath", "/Users/zhanggaoyuan/go/src/test/log/dog.log", "日志路径")
	goroutineNum := flag.Int("goroutineNum", 5, "消费者 goroutine 数量")
	l := flag.String("l", "/Users/zhanggaoyuan/go/src/test/log/acc.log", "日志打印文件路径")
	var params = cmdParams{*logFilePath, *goroutineNum}

	//Todo 打印日志.
	logFd, err := os.OpenFile(*l, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("os.OpenFile err = ", err)
		return
	}

	defer logFd.Close()

	log.Out = logFd
	log.Infoln("Exex start.")
	log.Infoln("params: logFilePath=%s, goroutineNum=%s", params.logFilePath, params.goroutineNum)

	//Todo 初始化 channel,用于数据传递.
	var logChan = make(chan string, params.goroutineNum*3)
	var pvChan = make(chan urlData, params.goroutineNum)
	var uvChan = make(chan urlData, params.goroutineNum)
	var storageChan = make(chan storageBlock, *goroutineNum)

	//Todo 日志消费者.
	go readFileLine(params, logChan)

	//Todo 创建一组日志处理.
	for i := 0; i < params.goroutineNum; i++ {
		go logConsumer(logChan, pvChan, uvChan)
	}

	//Todo 创建 pv uv 统计器.
	go pvConsumer(pvChan, storageChan)
	go uvConsumer(uvChan, storageChan)

	//Todo 创建储存器.
	go dataStorage(storageChan)

	time.Sleep(time.Second * 1000)
}

type digData struct {
	time  string
	url   string
	refer string
	ua    string
}

type urlData struct {
	data digData
	uid  string
}

type cmdParams struct {
	logFilePath  string
	goroutineNum int
}

type storageBlock struct {
	counterType  string
	storageModel string
	unode        urlNode
}

type urlNode struct {
}

func readFileLine(params cmdParams, logChan chan string) {

}

func logConsumer(logChan chan string, pvChan, uvChan chan urlData) {

}

func pvConsumer(pvChan chan urlData, storageChan chan storageBlock) {

}

func uvConsumer(uvChan chan urlData, storageChan chan storageBlock) {

}

func dataStorage(storageChan chan storageBlock) {

}
