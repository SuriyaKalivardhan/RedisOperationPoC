package main

import (
	"fmt"
	"time"

	"github.com/go-redis/redis/v7"
)

func livenessWorker(client *redis.Client) {
	threshold := 1 * time.Minute
	firstDownTime := make(map[string]time.Time, 1)
	var channels []string
	refresh := func() {
		channels = append(channels, "pythonproducerchannel")
	}
	refresh()

	for {
		select {
		case <-time.After(1 * time.Second):
			if len(channels) == 0 {
				continue
			}

			num, err := client.PubSubNumSub(channels...).Result()
			if err != nil {
				fmt.Printf("%v Could not check number of pipereplica subscribers: %s; sleeping 1s\n", time.Now().Format("2006-01-02T15:04:05"), err)
				time.Sleep(1 * time.Second)
				continue
			}

			for _, channel := range channels {

				if num[channel] == 0 {
					if firstDownTime[channel].IsZero() {
						firstDownTime[channel] = time.Now().UTC()
						fmt.Printf("%v No subscribers assignement %v\n", time.Now().Format("2006-01-02T15:04:05"), firstDownTime[channel].Format("2006-01-02T15:04:05"))
					}

					diff := time.Now().UTC().Sub(firstDownTime[channel])
					if diff > threshold {
						fmt.Printf("%v No subscribers difference before closing %v\n", time.Now().Format("2006-01-02T15:04:05"), diff)
						fmt.Printf("%v No subscribers, closing\n", time.Now().Format("2006-01-02T15:04:05"))
						return
					} else {
						fmt.Printf("%v No subscribers difference %v\n", time.Now().Format("2006-01-02T15:04:05"), diff)
					}

				} else {
					fmt.Printf("%v Number of subscribers is %v\n", time.Now().Format("2006-01-02T15:04:05"), num[channel])
					if !firstDownTime[channel].IsZero() {
						fmt.Printf("%v No subscribers resetting %v\n", time.Now().Format("2006-01-02T15:04:05"), firstDownTime[channel].Format("2006-01-02T15:04:05"))
						firstDownTime[channel] = time.Time{}
					}
				}
			}
		}
	}
}

func secondPhase(client *redis.Client, channels []string) {
	fmt.Println("........................IN Pahse 2...........................")
	select {
	case <-time.After(1 * time.Second):
		for {
			num, err := client.PubSubNumSub(channels...).Result()
			if err != nil {
				fmt.Printf("%v Could not check number of pipereplica subscribers: %s; sleeping 1s\n", time.Now().Format("2006-01-02T15:04:05"), err)
				time.Sleep(1 * time.Second)
				continue
			}

			for _, channel := range channels {
				if num[channel] == 0 {
					fmt.Printf("%v No subscribers, contiuing.....\n", time.Now().Format("2006-01-02T15:04:05"))
				} else {
					fmt.Printf("%v Number of subscribers is %v\n", time.Now().Format("2006-01-02T15:04:05"), num[channel])
				}
			}
		}
	}
}
