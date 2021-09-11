package redis_mq

import (
	"github.com/go-redis/redis"
	"net"
	"testing"
	"time"
)

func InitRedis() *redis.Client {
	host := "127.0.0.1"
	port := "6379"
	//建立链接
	redisCli := redis.NewClient(&redis.Options{
		Addr:net.JoinHostPort(host, port),
	})
	//测试连接
	if err := redisCli.Ping().Err(); err != nil {
		panic(err)
	}
	return redisCli
}

func TestRedisMq_PushMessage(t *testing.T) {
	redisCli := InitRedis()

	mq := NewRedisMq(redisCli, "test_mq", time.Second)

	row, err := mq.PushMessage("hello")
	if err != nil {
		t.Error(err)
		return
	}
	t.Log("row", row)
}

func TestRedisMq_GetMessage(t *testing.T) {
	redisCli := InitRedis()

	mq := NewRedisMq(redisCli, "test_mq", time.Second)

	msg, err := mq.GetMessage()
	if err != nil {
		t.Error(err)
		return
	}
	t.Log("get message", msg)

	row, err := mq.MessageAck(msg)
	if err != nil {
		t.Error(err)
		return
	}
	t.Log("ack rows", row)
}

func TestRedisMq_RepeatQueueBack(t *testing.T) {
	redisCli := InitRedis()

	mq := NewRedisMq(redisCli, "test_mq", time.Second, false)

	if err := mq.repeatQueueBackDo(mq.queueBack1Name, mq.queueBack2Name, time.Minute); err != nil {
		t.Error(err)
		return
	}
	t.Log("修补成功")
}

func TestRedisMq_RepeatQueueBackLoop(t *testing.T) {
	redisCli := InitRedis()

	//定义消息队列
	mq := NewRedisMq(redisCli, "test_mq", time.Second, false)

	//获取到数据的结果
	result := make(map[string]int64)
	//定义消费者
	go func() {
		for {
			msg, err := mq.GetMessage(time.Second)
			if err != nil {
				continue
			}
			//导入结果
			result[msg.MsgId]++
		}
	}()
}