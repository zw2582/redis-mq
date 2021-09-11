package redis_mq

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"github.com/go-redis/redis"
	"io"
	"log"
	"runtime/debug"
	"time"
)

//GoCoveryLoop 不死的捕获子协程开启
func GoCoveryLoop(title string, fun func()) {
	go func() {
		for {
			func() {
				defer func() {
					//捕获panic，并记录
					if e := recover(); e != nil {
						switch val := e.(type) {
						case error:
							log.Println("GoCoveryLoop捕获panic", map[string]interface{}{
								"title":title,"error":val,"stack":string(debug.Stack()),
							})
						default:
							log.Println("GoCoveryLoop捕获panic", map[string]interface{}{
								"title":title,"error":val,"stack":string(debug.Stack()),
							})
						}
					}
				}()
				//执行调用函数
				fun()
			}()
			//一旦发生失败，停顿5秒继续执行
			time.Sleep(time.Second * 5)
		}
	}()
}

//MultiServerControl 多服务器并发执行控制，针对无具体结束时间的并发控制
func MultiServerControl(redisCli *redis.Client, key string) (ok bool, err error) {
	d := time.Second * 5
	ok, err = redisCli.SetNX(key, 1, d).Result()
	//如果占用资源成功，则定时续约
	if ok {
		go func() {
			for {
				time.Sleep(d - time.Second)
				if err = redisCli.Expire(key, d).Err(); err != nil {
					log.Println("MultiServerControl 续约失败:", err.Error())
					break
				}
			}
		}()
	}
	return
}

//生成Guid字串
func UniqueId() string {
	b := make([]byte, 48)

	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		return ""
	}
	return Md5encode(base64.URLEncoding.EncodeToString(b))
}

//Md5encode md5编码
func Md5encode(src string) string {
	return fmt.Sprintf("%x", md5.Sum([]byte(src)))
}