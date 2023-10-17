package gormq

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type MessageAmpq struct {
	Exchange     string
	Routing_key  string
	Content_type string
	Data         []byte
}

type ExhangeOptions struct {
	Name         string
	ExchangeType string
	Keys         []string
	Durable      bool
	AutoDelete   bool
	Internal     bool
	NoWait       bool
	Args         amqp.Table
}

type QueueOption struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table
	QOS        int
}

func NewConnect(url string) (*Connection, error) {
	conn, err := Dial(url)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func NewChannelConsumer(wg *sync.WaitGroup, conn *Connection, exchOpt []ExhangeOptions, queuOpt QueueOption, cons chan MessageAmpq) (*Channel, error) {
	consumeCh, err := conn.Channel(wg, false, "consumer ", queuOpt.QOS)
	if err != nil {
		log.Panic(err)
	}

	channel := consumeCh.GetOrigChannel()
	for {
		if channel == nil {
			if consumeCh.IsClosed() {
				return nil, fmt.Errorf("close")
			}
			time.Sleep(time.Millisecond * 10)
			continue
		}
		break
	}

	_, err = channel.QueueDeclare(queuOpt.Name, queuOpt.Durable, queuOpt.Durable, queuOpt.Exclusive, queuOpt.NoWait, queuOpt.Args)
	if err != nil {
		return nil, err
	}

	for _, ex := range exchOpt {
		err = channel.ExchangeDeclare(ex.Name, amqp.ExchangeTopic, ex.Durable, ex.AutoDelete, ex.Internal, ex.NoWait, ex.Args)
		if err != nil {
			return nil, err
		}
		for _, key := range ex.Keys {
			if err := channel.QueueBind(queuOpt.Name, key, ex.Name, false, nil); err != nil {
				return nil, err
			}
		}
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		d, err := consumeCh.Consume(queuOpt.Name, "", false, false, false, false, nil)
		if err != nil {
			log.Panic(err)
		}

		for msg := range d {
			cons <- MessageAmpq{
				Exchange:     msg.Exchange,
				Routing_key:  msg.RoutingKey,
				Content_type: msg.ContentType,
				Data:         msg.Body,
			}
			err = msg.Ack(false)
			if err != nil {
				log.Println("Rabbitmq Consumer ACK", err)
			}
		}
	}()

	return consumeCh, nil
}

func NewChannelPublisher(wg *sync.WaitGroup, ctx context.Context, conn *Connection, pub chan MessageAmpq) (*Channel, error) {
	sendCh, err := conn.Channel(wg, false, "pub ", 0)
	if err != nil {
		return nil, err
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-pub:
				for {
					channel := sendCh.GetOrigChannel()
					if channel == nil {
						if sendCh.IsClosed() {
							return
						}
						time.Sleep(time.Millisecond * 10)
						continue
					}
					atomic.AddInt32(&sendCh.counterPubMsgRMQ, 1)
					err := channel.Publish(msg.Exchange, msg.Routing_key, false, false, amqp.Publishing{
						ContentType: msg.Content_type,
						Body:        msg.Data,
					})
					if err != nil {
						log.Printf("Rabbitmq Publish, err: %v", err)
						if sendCh.IsClosed() {
							return
						}
						time.Sleep(time.Second)
					} else {
						break
					}
				}
			}
		}
	}()
	return sendCh, nil
}

func publishWithContext(channel *amqp.Channel, msg *MessageAmpq) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return channel.PublishWithContext(
		ctx,
		msg.Exchange,    // Exchange
		msg.Routing_key, // Routing key
		false,           // Mandatory
		false,           // Immediate
		amqp.Publishing{
			ContentType: msg.Content_type,
			Body:        msg.Data,
		},
	)
}

func NewChannelPublisherWithAck(wg *sync.WaitGroup, ctx context.Context, conn *Connection, pub chan MessageAmpq) (*Channel, error) {
	sendCh, err := conn.Channel(wg, true, "pub ", 0)
	if err != nil {
		return nil, err
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer fmt.Println("end pub")
		notifyConfirmPub := make(chan amqp.Confirmation)
		var waitConfirm int32
		var lockConfirmPub int32
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer fmt.Println("end notify confirm pub")
			for {
				ch := sendCh.GetNotifyConfirm()
				if ch == nil {
					select {
					case <-ctx.Done():
						close(notifyConfirmPub)
						return
					case <-time.After(time.Millisecond * 100):
						continue
					}
				}

				select {
				case <-ctx.Done():
					close(notifyConfirmPub)
					return
				case c, ok := <-ch:
					if ok {
						if atomic.LoadInt32(&waitConfirm) == 1 {
							atomic.StoreInt32(&lockConfirmPub, 1)
							notifyConfirmPub <- c
							atomic.StoreInt32(&lockConfirmPub, 0)
						}
					} else {
						time.Sleep(time.Millisecond * 100)
					}
				}
			}
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-pub:
				for {
					channel := sendCh.GetOrigChannel()
					if channel == nil {
						if sendCh.IsClosed() {
							return
						}
						time.Sleep(time.Millisecond * 10)
						continue
					}
					atomic.AddInt32(&sendCh.counterPubMsgRMQ, 1)
					atomic.StoreInt32(&waitConfirm, 1)
					err := publishWithContext(channel, &msg)
					if err != nil {
						atomic.StoreInt32(&waitConfirm, 0)
						log.Println("Rabbitmq Publish no confirm")
						if sendCh.IsClosed() {
							return
						}
						time.Sleep(time.Second * time.Duration(delay))
					} else {
						// only ack the source delivery when the destination acks the publishing
						var ack bool
						for {
							select {
							case confirm := <-notifyConfirmPub:
								if confirm.Ack {
									ack = true
									atomic.StoreInt32(&waitConfirm, 0)
									//log.Println("Rabbitmq Push confirmed!")
								}
								atomic.StoreInt32(&waitConfirm, 0)
							case <-time.After(time.Second * time.Duration(delay)):
								atomic.StoreInt32(&waitConfirm, 0)
							}
							if atomic.LoadInt32(&lockConfirmPub) == 1 {
								runtime.Gosched()
								if atomic.LoadInt32(&lockConfirmPub) == 1 {
									debugf("atomic.LoadInt32(&lockConfirmPub) == 1")
									continue
								}
							}
							break
						}
						if ack {
							break
						}
						log.Println("Rabbitmq Push didn't confirm. Retrying...", msg.Exchange, msg.Routing_key)
					}
				}
			}
		}
	}()

	return sendCh, nil
}
