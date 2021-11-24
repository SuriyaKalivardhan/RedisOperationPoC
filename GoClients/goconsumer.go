package main

import (
	"fmt"
	"time"

	"github.com/go-redis/redis/v7"
)

func goConsumer(client *redis.Client) {
	//const goProducerChannel = "goproducerchannel"
	const goProducerChannel = "AzureRedisEvents"
	goChannel := client.Subscribe(goProducerChannel)
	ch_ := goChannel.Channel()
	for {
		// msg, err := goChannel.Receive()
		// if err != nil {
		// 	fmt.Printf("%v [CONSUMER] %v\n", time.Now().Format("2006-01-02T15:04:05"), err)
		// 	client, _ = connectRedis()
		// 	goChannel = client.Subscribe(goProducerChannel)
		// } else {
		// 	fmt.Printf("%v [CONSUMER] %v\n", time.Now().Format("2006-01-02T15:04:05"), msg)
		// }
		select {
		case msg := <-ch_:
			{
				fmt.Printf("[REDIS EVENT] %v\n", msg)
				//fmt.Printf("%v [CONSUMER] %v\n", time.Now().Format("2006-01-02T15:04:05"), msg)
			}
		}
	}
}

func pythonConsumer(client *redis.Client) {
	const pythonProducerChannel = "pythonproducerchannel"
	goChannel := client.Subscribe(pythonProducerChannel)
	ch_ := goChannel.Channel()

	go func() {
		for {
			num, _ := client.PubSubNumSub(pythonProducerChannel).Result()
			fmt.Printf("[PUBNUMSUB] %v\n", num[pythonProducerChannel])
			time.Sleep(1 * time.Second)
		}
	}()

	for {
		// msg, err := goChannel.Receive()
		// if err != nil {
		// 	fmt.Printf("%v [CONSUMER] %v\n", time.Now().Format("2006-01-02T15:04:05"), err)
		// 	client, _ = connectRedis()
		// 	goChannel = client.Subscribe(goProducerChannel)
		// } else {
		// 	fmt.Printf("%v [CONSUMER] %v\n", time.Now().Format("2006-01-02T15:04:05"), msg)
		// }
		select {
		case msg := <-ch_:
			{
				fmt.Printf("%v [CONSUMER] %v\n", time.Now().Format("2006-01-02T15:04:05"), msg)
			}
		}
	}
}
