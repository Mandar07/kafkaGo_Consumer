package main

import (
	"context"
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	ctx := context.Background()
	consume(ctx)
}

func consume(ctx context.Context) {

	configMap := make(map[string]kafka.ConfigValue)
	configMap["bootstrap.servers"] = "localhost:9092"
	configMap["group.id"] = "groupID_page" // to avoid groupID error
	var kConfigMap kafka.ConfigMap = kafka.ConfigMap(configMap)

	c, err := kafka.NewConsumer(&kConfigMap)
	if err != nil {
		panic(err)
	}

	topic := "selftuts"

	c.SubscribeTopics([]string{topic}, nil)

	exec := true
	msg_count := 0

	for exec == true {
		ev := c.Poll(0)
		switch e := ev.(type) {
		case *kafka.Message:
			msg_count += 1
			go func() {
				topicPars, err := c.Commit()
				if err != nil {
					fmt.Printf("Error while commiting message %s \n", err)
				} else {
					for i := range topicPars {
						fmt.Printf("Comitted topic:%s | offset:%d on Parition:%d \n", *topicPars[i].Topic, topicPars[i].Offset, topicPars[i])
					}

				}
			}()
			fmt.Printf(" Message received Key:%s | Value:%s\n", string(e.Key), string(e.Value))

		case kafka.Error:
			fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
			exec = false
		default:
			//fmt.Printf("I am waiting \n") // dont use default to avoid indefinite console print
		}
	}
	fmt.Printf("Info : Closing consumer\n")
	c.Close()
}
