package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	goutils "github.com/lasaleks/go-utils"
	"github.com/lasaleks/gormq"
)

var (
	CH_PUBLISH_MSG_AMPQ chan gormq.MessageAmpq
)

func main() {
	var wg sync.WaitGroup
	ctx := context.Background()

	CH_PUBLISH_MSG_AMPQ = make(chan gormq.MessageAmpq, 100)

	goutils.CreatePidFile("/tmp/test_gormq_cons.pid")
	defer os.Remove("/tmp/test_gormq_cons.pid")
	gormq.Debug = true
	conn_rmq, err := gormq.NewConnect("amqp://rabbit:rabbitie@localhost:5672/")
	if err != nil {
		log.Panicln("connect rabbitmq", err)
	} else {
		log.Println("RabbitMQ Connected!", conn_rmq.LocalAddr(), conn_rmq.RemoteAddr())
	}
	ctx_rmq, cancel_rmq := context.WithCancel(ctx)
	chSend, err := gormq.NewChannelPublisherWithAck(&wg, ctx_rmq, conn_rmq, CH_PUBLISH_MSG_AMPQ)
	if err != nil {
		log.Println("error init Channel Publisher!", err)
	} else {
		log.Println("Channel Publisher Ok!")
	}

	exit := false
	go func() {
		for !exit {
			CH_PUBLISH_MSG_AMPQ <- gormq.MessageAmpq{
				Exchange:     "testgorm",
				Routing_key:  "data.key",
				Content_type: "text/plain",
				Data:         []byte("test message"),
			}
			time.Sleep(time.Second * 1)
		}
	}()

	f_shutdown := func(ctx context.Context) {
		chSend.Close()
		cancel_rmq()
		exit = true
	}
	wg.Add(1)
	go goutils.WaitSignalExit(&wg, ctx, f_shutdown)
	wg.Wait()
	fmt.Println("End")
}
