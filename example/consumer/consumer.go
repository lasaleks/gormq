package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"

	goutils "github.com/lasaleks/go-utils"
	"github.com/lasaleks/gormq"
)

var (
	CH_MSG_AMPQ chan gormq.MessageAmpq
)

func main() {
	var wg sync.WaitGroup
	ctx := context.Background()
	CH_MSG_AMPQ = make(chan gormq.MessageAmpq, 1)

	goutils.CreatePidFile("/tmp/test_gormq_cons.pid")
	defer os.Remove("/tmp/test_gormq_cons.pid")

	conn_rmq, err := gormq.NewConnect("amqp://rabbit:rabbitie@localhost:5672/")
	if err != nil {
		log.Panicln("connect rabbitmq", err)
	}
	gormq.Debug = true
	chCons, err := gormq.NewChannelConsumer(
		&wg, conn_rmq, []gormq.ExhangeOptions{
			{
				Name:         "testgorm",
				ExchangeType: "topic",
				Keys: []string{
					"#",
				},
			},
		},
		gormq.QueueOption{
			QOS:  50,
			Name: "test_consumer_gormq3",
		},
		CH_MSG_AMPQ,
	)
	if err != nil {
		log.Panicln(err)
	}
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		fmt.Println("Run Consumer")
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-CH_MSG_AMPQ:
				fmt.Printf("%s %s %q\n", msg.Exchange, msg.Routing_key, msg.Data)
			}
		}
	}()
	f_shutdown := func(ctx context.Context) {
		chCons.Close()
		cancel()
	}
	wg.Add(1)
	go goutils.WaitSignalExit(&wg, ctx, f_shutdown)
	wg.Wait()
	fmt.Println("End")
}
