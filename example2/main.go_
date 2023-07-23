package main

import (
	"context"
	"fmt"
	"log"
	"myutils"
	"sync"
	"time"

	"bitbucket.org/lasaleks/gormq3"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	ctx := context.Background()
	wg := sync.WaitGroup{}
	myutils.CreatePidFile("/tmp/gormqeExample.pid")

	CH_CONSUMER_MSG := make(chan gormq3.MessageAmpq, 1)
	CH_PUB_MSG := make(chan gormq3.MessageAmpq, 1)

	conn_rmq, err := gormq3.NewConnect("amqp://rabbit:rabbitie@192.168.67.3/?heartbeat=10")
	if err != nil {
		log.Panicln("connect rabbitmq", err)
	}
	ctx_rmq, cancel_rmq := context.WithCancel(ctx)
	chPub, err := gormq3.NewChannelPublisher(&wg, ctx_rmq, conn_rmq, CH_PUB_MSG)
	if err != nil {
		log.Panicln("connect rabbitmq", err)
	}

	chCons, err := gormq3.NewChannelConsumer(
		&wg, conn_rmq, []gormq3.ExhangeOptions{{
			Name:         "TestGORMQ2",
			ExchangeType: amqp.ExchangeTopic,
			Keys: []string{
				"TesGoRMQPub.#",
			},
		}},
		gormq3.QueueOption{
			QOS:  500,
			Name: "testgormq3",
		},
		CH_CONSUMER_MSG,
	)

	ctx_send, cancel_send := context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer fmt.Println("end CH_PUB_MSG")
		idx := 0
		for {
			select {
			case <-ctx_send.Done():
				return
			case <-time.After(time.Second * 1):
				CH_PUB_MSG <- gormq3.MessageAmpq{
					Exchange:    "TestGORMQ2",
					Routing_key: fmt.Sprintf("TesGoRMQPub.gormq3.noack.%d", idx),
				}
				idx++
			}

		}
	}()

	ctx_recv, cancel_recv := context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer fmt.Println("end CH_CONSUMER_MSG")
		//idx := 0
		for {
			select {
			case <-ctx_recv.Done():
				return
			case msg := <-CH_CONSUMER_MSG:
				fmt.Printf("recv %++q\n", msg)
			}
		}
	}()

	f_shutdown := func(ctx context.Context) {
		fmt.Println("ShutDown")
		conn_rmq.Close()
		chCons.Close()
		chPub.Close()
		cancel_send()
		cancel_rmq()
		cancel_recv()
		fmt.Println("ShutDown end")
	}
	wg.Add(1)
	go myutils.WaitSignalExit(&wg, ctx, f_shutdown)
	wg.Wait()
	fmt.Println("END")
}
