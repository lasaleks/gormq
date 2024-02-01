package gormq

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"sync/atomic"

	amqp "github.com/rabbitmq/amqp091-go"
)

func getReconnDelay() int {
	if os.Getenv("AMQP_RECONN_DELAY_SECONDS") == "" {
		return 3
	}
	delay, err := strconv.Atoi(os.Getenv("AMQP_RECONN_DELAY_SECONDS"))
	if err != nil {
		fmt.Println("Cannot convert env `AMQP_RECONN_DELAY_SECONDS` to a number, default to 3.")
		return 3
	}
	return delay
}

var delay = getReconnDelay() // reconnect after delay seconds

// Connection amqp.Connection wrapper
type Connection struct {
	connection *amqp.Connection
	mu         sync.Mutex
}

func (c *Connection) GetConnect() *amqp.Connection {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connection
}

func (c *Connection) LocalAddr() net.Addr {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connection.LocalAddr()
}

func (c *Connection) RemoteAddr() net.Addr {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connection.RemoteAddr()
}

func (c *Connection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connection.Close()
}

// Channel wrap amqp.Connection.Channel, get a auto reconnect channel
func (c *Connection) Channel(wg *sync.WaitGroup, sendConfirm bool, name string, qos int) (*Channel, error) {
	connect := c.GetConnect()

	ch, err := connect.Channel()
	if err != nil {
		return nil, err
	}

	channel := &Channel{
		mu: sync.Mutex{},
	}

	if sendConfirm {
		notifyPub := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
		if err := ch.Confirm(false); err != nil {
			debugf("%sinit Confirm err:%s", name, err)
		}
		if qos != 0 {
			err = ch.Qos(qos, 0, false)
			if err != nil {
				fmt.Println("error", err)
			}
		}
		channel.mu.Lock()
		channel.notifyConfirm = notifyPub
		channel.channel = ch
		channel.mu.Unlock()
	} else {
		if qos != 0 {
			err = ch.Qos(qos, 0, false)
			if err != nil {
				fmt.Println("error", err)
			}
		}
		channel.mu.Lock()
		channel.channel = ch
		channel.mu.Unlock()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer debugf("Channel %s end", name)
		for {
			reason, ok := <-channel.channel.NotifyClose(make(chan *amqp.Error))
			// exit this goroutine if closed by developer
			if !ok || channel.IsClosed() {
				debugf("%schannel close", name)
				channel.GetOrigChannel().Close()
				return
			}
			debugf("%schannel closed, reason: %v", reason, name)

			// reconnect if not closed by developer
			for {
				// wait 1s for connection reconnect
				time.Sleep(time.Duration(delay) * time.Second)

				connect := c.GetConnect()
				ch, err := connect.Channel()
				if err == nil {
					if sendConfirm {
						notifyPub := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
						if err := ch.Confirm(false); err != nil {
							debugf("%sinit Confirm err:%s", name, err)
						}
						if qos != 0 {
							err = ch.Qos(qos, 0, false)
							if err != nil {
								debugf("set qos error:%s", err)
							}
						}
						channel.mu.Lock()
						channel.notifyConfirm = notifyPub
						channel.channel = ch
						channel.mu.Unlock()
					} else {
						if qos != 0 {
							err = ch.Qos(qos, 0, false)
							if err != nil {
								debugf("set qos error: %s", err)
							}
						}
						channel.mu.Lock()
						channel.channel = ch
						channel.mu.Unlock()
					}
					debugf("%schannel recreate success", name)
					break
				}
				debugf("%schannel recreate failed, err: %v", name, err)
			}
		}
	}()

	return channel, nil
}

// Channel amqp.Channel wapper
type Channel struct {
	Name             string
	channel          *amqp.Channel
	closed           int32
	notifyConfirm    chan amqp.Confirmation
	mu               sync.Mutex
	counterPubMsgRMQ int32 // кол-во отправленных сообщений через rabbitmq
}

func (ch *Channel) GetCounterPubMsg() int32 {
	return atomic.LoadInt32(&ch.counterPubMsgRMQ)
}

func (ch *Channel) GetOrigChannel() *amqp.Channel {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return ch.channel
}

func (ch *Channel) GetNotifyConfirm() chan amqp.Confirmation {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return ch.notifyConfirm
}

// IsClosed indicate closed by developer
func (ch *Channel) IsClosed() bool {
	return atomic.LoadInt32(&ch.closed) == 1
}

// Close ensure closed flag set
func (ch *Channel) Close() error {
	if ch.IsClosed() {
		return amqp.ErrClosed
	}

	atomic.StoreInt32(&ch.closed, 1)

	return ch.channel.Close()
}

// Consume wrap amqp.Channel.Consume, the returned delivery will end only when channel closed by developer
func (ch *Channel) Consume(exchOpt []ExhangeOptions, queuOpt QueueOption, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	deliveries := make(chan amqp.Delivery)

	go func() {
		defer close(deliveries)
		for {
			channel := ch.GetOrigChannel()
			for {
				if channel == nil {
					if ch.IsClosed() {
						break
					}
					time.Sleep(time.Duration(delay) * time.Second)
					continue
				}
				break
			}

			_, err := channel.QueueDeclare(queuOpt.Name, queuOpt.Durable, queuOpt.Durable, queuOpt.Exclusive, queuOpt.NoWait, queuOpt.Args)
			if err != nil {
				debugf("QueueDeclare %s failed, err: %v", queuOpt.Name, err)
				if ch.IsClosed() {
					break
				}
				time.Sleep(time.Duration(delay) * time.Second)
				continue
			}

			for _, ex := range exchOpt {
				err = channel.ExchangeDeclare(ex.Name, amqp.ExchangeTopic, ex.Durable, ex.AutoDelete, ex.Internal, ex.NoWait, ex.Args)
				if err != nil {
					debugf("ExchangeDeclare %s failed, err: %v", ex.Name, err)
					break
				}
				for _, key := range ex.Keys {
					if err = channel.QueueBind(queuOpt.Name, key, ex.Name, false, nil); err != nil {
						debugf("QueueBind %s %s %s failed, err: %v", queuOpt.Name, key, ex.Name, err)
						break
					}
				}
				if err != nil {
					break
				}
			}
			if err != nil {
				debugf("ExchangeDeclare|QueueDeclare failed, err: %v", err)
				if ch.IsClosed() {
					break
				}
				time.Sleep(time.Duration(delay) * time.Second)
				continue
			}

			delivery, err := channel.Consume(queuOpt.Name, consumer, autoAck, exclusive, noLocal, noWait, args)
			if err != nil {
				debugf("consume failed, err: %v", err)
				if ch.IsClosed() {
					break
				}
				time.Sleep(time.Duration(delay) * time.Second)
				continue
			}

			for msg := range delivery {
				deliveries <- msg
			}

			// sleep before IsClose call. closed flag may not set before sleep.
			time.Sleep(time.Duration(delay) * time.Second)

			if ch.IsClosed() {
				//close(deliveries)
				break
			}
		}
	}()
	return deliveries, nil
}

// Dial wrap amqp.Dial, dial and get a reconnect connection
func Dial(url string) (*Connection, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	connection := &Connection{
		connection: conn,
		mu:         sync.Mutex{},
	}
	setStatusOnLine(true)
	go func() {
		for {
			connect := connection.GetConnect()

			reason, ok := <-connect.NotifyClose(make(chan *amqp.Error))
			// exit this goroutine if closed by developer
			setStatusOnLine(false)
			if !ok {
				debugf("connection closed")
				break
			}
			debugf("connection closed, reason: %v", reason)
			// reconnect if not closed by developer
			for {
				// wait 1s for reconnect
				time.Sleep(time.Duration(delay) * time.Second)
				conn, err := amqp.Dial(url)
				if err == nil {
					connection.mu.Lock()
					connection.connection = conn
					connection.mu.Unlock()
					setStatusOnLine(true)
					debugf("reconnect success")
					break
				}
				setStatusOnLine(false)

				debugf("reconnect failed, err: %v", err)
			}
		}
	}()

	return connection, nil
}

// DialCluster with reconnect
func DialCluster(urls []string) (*Connection, error) {
	nodeSequence := 0
	conn, err := amqp.Dial(urls[nodeSequence])

	if err != nil {
		return nil, err
	}
	connection := &Connection{
		connection: conn,
	}

	go func(urls []string, seq *int) {
		for {
			connect := connection.GetConnect()
			reason, ok := <-connect.NotifyClose(make(chan *amqp.Error))
			if !ok {
				debugf("connection closed")
				break
			}
			debugf("connection closed, reason: %v", reason)

			// reconnect with another node of cluster
			for {
				time.Sleep(time.Duration(delay) * time.Second)

				newSeq := next(urls, *seq)
				*seq = newSeq

				conn, err := amqp.Dial(urls[newSeq])
				if err == nil {
					connection.mu.Lock()
					connection.connection = conn
					connection.mu.Unlock()
					debugf("reconnect success")
					break
				}

				debugf("reconnect failed, err: %v", err)
			}
		}
	}(urls, &nodeSequence)

	return connection, nil
}

// Next element index of slice
func next(s []string, lastSeq int) int {
	length := len(s)
	if length == 0 || lastSeq == length-1 {
		return 0
	} else if lastSeq < length-1 {
		return lastSeq + 1
	} else {
		return -1
	}
}
