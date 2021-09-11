package redis_mq

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	"log"
	"time"
)

//RedisMq redis消息队列
type RedisMq struct {
	queueName      string        //消息队列名称
	ackSetName     string        //应答队列
	queueBack1Name string        //修复队列1名称
	queueBack2Name string        //修复队列2名称
	queueBack3Name string        //修复队列3名称
	repeatDuration time.Duration //修复间隔时间
	redisCli       *redis.Client //redis连接客户端
}

//RedisMqMsg 消息体
type RedisMqMsg struct {
	MsgId string      `json:"msg_id"` //消息唯一id
	Body  interface{} `json:"body"`   //内容
	CTime time.Time   `json:"c_time"` //消息发送时间
	AckCnt int `json:"ack_cnt"` //回执次数
}

func (this RedisMqMsg) String() (string, error) {
	raw, err := json.Marshal(this)
	return string(raw), err
}

func DecodeMqMsg(raw string) (msg RedisMqMsg, err error) {
	err = json.Unmarshal([]byte(raw), &msg)
	return
}

//NewRedisMq 新建一个redis消息队列
func NewRedisMq(redisCli *redis.Client, queueName string, repeatDuration time.Duration, autoRepeat ...bool) RedisMq {
	if redisCli == nil {
		panic(errors.New("请为RedisMq传递有效的redis客户端"))
	}
	if err := redisCli.Ping().Err(); err != nil {
		panic(err)
	}
	cli := RedisMq{
		queueName:      queueName,
		ackSetName:     fmt.Sprintf("%s_ack_mq_35862714", queueName),
		queueBack1Name: fmt.Sprintf("%s_backend1_mq_35862714", queueName),
		queueBack2Name: fmt.Sprintf("%s_backend2_mq_35862714", queueName),
		queueBack3Name: fmt.Sprintf("%s_backend3_mq_35862714", queueName),
		repeatDuration: repeatDuration,
		redisCli:       redisCli,
	}

	//开启异步修补队列
	at := true
	if len(autoRepeat) > 0 {
		at = autoRepeat[0]
	}
	if at {
		cli.RepeatQueueBack()
	}

	return cli
}

//PushMessage 塞入消息
func (this *RedisMq) PushMessage(body interface{}) (int64, error) {
	if body == nil {
		return 0, errors.New("请投入有效消息")
	}

	msg := RedisMqMsg{
		MsgId: UniqueId(),
		Body:  body,
		CTime: time.Now(),
	}
	raw, err := msg.String()
	if err != nil {
		return 0, err
	}

	return this.redisCli.LPush(this.queueName, raw).Result()
}

//GetMessage 获取消息
//	blockDration 阻塞一段时间获取
// 返回err为redis.nil时，代表没有获取到数据
func (this *RedisMq) GetMessage(blockDuration ...time.Duration) (msg RedisMqMsg, err error) {
	var raw string
	if len(blockDuration) == 0 {
		raw, err = this.redisCli.RPopLPush(this.queueName, this.queueBack1Name).Result()
	} else {
		raw, err = this.redisCli.BRPopLPush(this.queueName, this.queueBack1Name, blockDuration[0]).Result()
	}

	if err != nil {
		return msg, err
	}
	msg, err = DecodeMqMsg(raw)
	return
}

//MessageAck 消息
func (this *RedisMq) MessageAck(msg RedisMqMsg) (int64, error) {
	if msg.MsgId == "" {
		return 0, errors.New("回执无效msg")
	}
	return this.redisCli.SAdd(this.ackSetName, msg.MsgId).Result()
}

//RepeatQueueBack 修复队列
func (cli *RedisMq) RepeatQueueBack() {
	//修复队列任务
	GoCoveryLoop("修复队列:"+cli.queueBack1Name, func() {
		if err := cli.repeatQueueBackDo(cli.queueBack1Name, cli.queueBack2Name, cli.repeatDuration); err != nil {
			log.Println("修复1号队列失败", "err:", err.Error())
		}
	})
	GoCoveryLoop("修复队列:"+cli.queueBack2Name, func() {
		if err := cli.repeatQueueBackDo(cli.queueBack2Name, cli.queueBack3Name, cli.repeatDuration*2); err != nil {
			log.Println("修复2号队列失败", "err:", err.Error())
		}
	})
	GoCoveryLoop("修复队列:"+cli.queueBack3Name, func() {
		if err := cli.repeatQueueBackDo(cli.queueBack3Name, "", cli.repeatDuration*3); err != nil {
			log.Println("修复3号队列失败", "err:", err.Error())
		}
	})
}

//repeatQueueBack1 修复1号队列
func (this *RedisMq) repeatQueueBack1() error {
	//多进程并发拦截
	ok, err := MultiServerControl(this.redisCli, fmt.Sprintf("%s_filter", this.queueBack1Name))
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("没有获取到执行修复队列权利")
	}

	//定义临时存取队列
	tempKey := this.queueBack1Name + "_tmp"
	defer func() {
		//清理temp数据
		this.redisCli.Del(tempKey)
	}()

	//修复上一次temp中断的msg
	if this.redisCli.LLen(tempKey).Val() > 0 {
		this.redisCli.RPopLPush(tempKey, this.queueBack1Name)
	}

	for {
		//遍历修复队列的数据，判断需要等待的时间
		raw, err := this.redisCli.RPopLPush(this.queueBack1Name, tempKey).Result()
		if err != nil && err != redis.Nil {
			return err
		}

		//如果没有则停顿一定时间
		if raw == "" {
			time.Sleep(this.repeatDuration)
			continue
		}

		//解析消息，并判断需要等待的时间
		msg, err := DecodeMqMsg(raw)
		if err != nil {
			return err
		}
		msg.AckCnt++

		pipeCmds := make([]redis.Cmder, 0)

		switch {
		case msg.AckCnt == 2:
			//todo 塞入2号修复队列，删除临时空间
			continue
		case msg.AckCnt == 3:
			//todo 塞入3号修复队列，删除临时空间
			continue
		case msg.AckCnt > 3:
			//todo 丢弃临时空间
			continue
		default:

		}

		//仅仅等待时间超过5秒才需要等待，如果还需要等待时间不足5秒，则直接执行修复逻辑
		waitDuration := msg.CTime.Add(this.repeatDuration).Sub(time.Now())
		if waitDuration > time.Second*5 {
			time.Sleep(waitDuration)
		}

		//等待一定时间后，检查ack集合是否已经出现该msg，出现则同时删除两个队列的msg,否则将该msg移动到下一级修复队列，并将msg修复到正常队列
		exist, err := this.redisCli.SIsMember(this.ackSetName, msg.MsgId).Result()
		if err != nil {
			return err
		}

		if exist {
			//同时删除集合和修复队列中的msg
			pipeCmds, err = this.redisCli.Pipelined(func(pl redis.Pipeliner) error {
				pl.SRem(this.ackSetName, msg.MsgId)
				pl.Del(tempKey)
				return nil
			})
		} else {
			//修复消息到正常队列
			raw, _ = msg.String()
			pipeCmds, err = this.redisCli.Pipelined(func(pl redis.Pipeliner) error {
				pl.RPush(this.queueName, raw)
				pl.Del(tempKey)
				return nil
			})
		}
		for _, val := range pipeCmds {
			if err = val.Err(); err != nil {
				return err
			}
		}
	}
}

//repeatQueueBackDo 修复队列
func (this *RedisMq) repeatQueueBackDo(currentBackName, nextBackName string, duration time.Duration) error {







}
