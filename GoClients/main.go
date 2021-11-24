package main

import (
	"crypto/tls"
	"fmt"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/pkg/errors"
)

func main() {
	client, _ := connectRedis()
	go produce(client)
	//go goConsumer(client)
	//go pythonConsumer(client)
	//go livenessWorker(client)
	forever()
}

func connectRedis() (*redis.Client, error) {
	host := "abc.redis.cache.windows.net:6380"
	passwd := "7z...="
	maxRetries := 100
	retryTime := 1 * time.Second
	attemptCount := 0
	for {
		redisOptions := &redis.Options{Addr: host, Password: passwd}
		redisOptions.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		client := redis.NewClient(redisOptions)

		if _, err := client.Ping().Result(); err != nil {
			if attemptCount == maxRetries {
				return nil, errors.Wrapf(err, "Max retries (%d) connecting to Redis at %s", maxRetries, host)
			}
			// Retry
			attemptCount += 1
			fmt.Println("[INIT]", err)
			time.Sleep(retryTime)
			continue
		} else {
			fmt.Println("[INIT] Successful client")
			return client, nil
		}
	}
}

func forever() {
	for {
		select {}
	}
}
