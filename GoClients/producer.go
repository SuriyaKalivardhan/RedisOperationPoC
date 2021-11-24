package main

import (
	"fmt"
	"time"

	"github.com/go-redis/redis/v7"
)

func produce(client *redis.Client) {
	const goProducerChannel = "goproducerchannel"
	counter := 0
	for {
		nSub, err := client.Publish(goProducerChannel, counter).Result()
		if err != nil {
			fmt.Printf("%v [PRODUCER] %v\n", time.Now().Format("2006-01-02T15:04:05"), err)
			//client, _ = connectRedis()
		} else {
			fmt.Printf("%v [PRODUCER] %v %v\n", time.Now().Format("2006-01-02T15:04:05"), counter, nSub)
		}
		time.Sleep(5 * time.Second)
		counter++
	}
}
