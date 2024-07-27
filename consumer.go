package main

import (
	"context"
	"github.com/IBM/sarama"
	"log"
	"time"
)

type exampleConsumerGroupHandler struct{}

func (handler exampleConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (handler exampleConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h exampleConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		log.Printf("Received message: %s\n", string(msg.Value))
		sess.MarkMessage(msg, "")
	}
	return nil
}

func main() {
	brokers := []string{"localhost:9093"}
	groupId := "consumer-group"
	config := sarama.NewConfig()
	config.Version = sarama.V2_0_0_0
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second

	consumerGroup, err := sarama.NewConsumerGroup(brokers, groupId, config)
	if err != nil {
		log.Panicf("Error creating consumer group %v", err)
	}

	ctx := context.Background()
	for {
		err := consumerGroup.Consume(ctx, []string{"post-likes"}, exampleConsumerGroupHandler{})
		if err != nil {
			log.Panicf("Error from consumer: %v", err)
		}
	}
}
