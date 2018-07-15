package main

import (
	"flag"
	"time"
	"github.com/sirupsen/logrus"
	"os"
	"fmt"
	"bufio"
	"io"
	"strings"
	"github.com/mgutz/str"
	"net/url"
	"crypto/md5"
	"encoding/hex"
)

const HEADER_DIG = " /dog?"

const URL = "http://blog.test/?"

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

func readFileLine(params cmdParams, logChan chan string) (err error) {
	//Todo 文件
	f, err := os.Open(params.logFilePath)
	if err != nil {
		log.Warningf("readFileLine open file err= %s", err)
	}

	defer f.Close()

	count := 0

	//新建一个缓冲区,把内容先放进缓冲区
	r := bufio.NewReader(f)
	for {
		//遇到'\n'结束读取, 但是'\n'也读取进入
		buf, err := r.ReadString('\n')
		if err != nil {
			if err == io.EOF { //文件已经结束
				time.Sleep(time.Second * 10)
				log.Infoln("文件读取完毕~ 休息10秒")
			} else {
				log.Warningf("文件内容读取错误: err=%s", err)
			}
		}

		logChan <- buf
		log.Infof("buf = %s", buf)
		count ++

		if count%(1000*params.goroutineNum) == 0 {
			log.Infof("readFileLine line: %d", count)
		}
	}
}

func logConsumer(logChan chan string, pvChan, uvChan chan urlData) {
	for logStr := range logChan {
		//切割日志字符串,扣除需要上报的信息.
		data := cutLogFetchData(logStr)

		hasher := md5.New()
		hasher.Write([]byte(data.refer + data.ua))
		uid := hex.EncodeToString(hasher.Sum(nil))

		uDta := urlData{data, uid}
		fmt.Printf("uDta: uid= %s, data = %s\n", uDta.uid, uDta.data)
		pvChan <- uDta
		uvChan <- uDta
	}
}

func cutLogFetchData(logStr string) digData {
	logStr = strings.TrimSpace(logStr)
	pos := str.IndexOf(logStr, HEADER_DIG, 0)
	if pos == -1 {
		return digData{}
	}
	pos += len(HEADER_DIG)
	pos2 := str.IndexOf(logStr, " HTTP", pos)
	if pos2 == -1 {
		return digData{}
	}
	d := str.Substr(logStr, pos, pos2-pos)

	urlInfo, err := url.Parse(URL + d)
	if err != nil {
		return digData{}
	}

	data := urlInfo.Query()

	return digData{
		time:  data.Get("time"),
		url:   data.Get("url"),
		refer: data.Get("refer"),
		ua:    data.Get("ua"),
	}
}

func pvConsumer(pvChan chan urlData, storageChan chan storageBlock) {

}

func uvConsumer(uvChan chan urlData, storageChan chan storageBlock) {

}

func dataStorage(storageChan chan storageBlock) {

}
