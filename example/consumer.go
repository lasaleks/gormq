package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	"bitbucket.org/lasaleks/gormq3"
	"github.com/lasaleks/ie_common_utils_go"
)

var (
	CH_MSG_AMPQ chan gormq3.MessageAmpq
)

func main() {
	var wg sync.WaitGroup
	ctx := context.Background()
	CH_MSG_AMPQ = make(chan gormq3.MessageAmpq, 1)

	conn_rmq, err := gormq3.NewConnect("amqp://rabbit:rabbitie@localhost:5672/")
	if err != nil {
		log.Panicln("connect rabbitmq", err)
	}

	chCons, err := gormq3.NewChannelConsumer(
		&wg, conn_rmq, []gormq3.ExhangeOptions{
			{
				Name:         "testgorm",
				ExchangeType: "topic",
				Keys: []string{
					"#",
				},
			},
		},
		gormq3.QueueOption{
			QOS:  50,
			Name: "test_consumer_gormq3",
		},
		CH_MSG_AMPQ,
	)
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		fmt.Println("Run Consumer")
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-CH_MSG_AMPQ:
				fmt.Println("msg:", msg)
			}
		}
	}()
	f_shutdown := func(ctx context.Context) {
		chCons.Close()
		cancel()
	}
	wg.Add(1)
	go ie_common_utils_go.WaitSignalExit(&wg, ctx, f_shutdown)
	// ждем освобождение горутин
	wg.Wait()
}
