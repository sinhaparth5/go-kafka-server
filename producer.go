package main

import (
	"encoding/json"
	"github.com/IBM/sarama"
	"log"
	"math/rand"
	"os"
)

type Message struct {
	userId     int    `json:"user_id"`
	postId     string `json:"post_id"`
	UserAction string `json:"user_action"`
}

func main() {
	brokers := []string{"localhost:9093"}
	producer, err := sarama.NewSyncProducer(brokers, nil)
	if err != nil {
		log.Fatalln("Failed to start Sarama producer:", err)
		os.Exit(1)
	}

	userId := [5]int{100001, 100002, 100003, 100004, 100005}
	postId := [5]string{"POST1", "POST2", "POST3", "POST4", "POST5"}
	userAction := [5]string{"love", "like", "hate", "smile", "cry"}

	for {
		message := Message{
			userId:     userId[rand.Intn(len(userId))],
			postId:     postId[rand.Intn(len(postId))],
			UserAction: userAction[rand.Intn(len(userAction))],
		}

		jsonMessage, err := json.Marshal(message)
		if err != nil {
			log.Fatalln("Failed to marshal message:", err)
			os.Exit(1)
		}

		msg := &sarama.ProducerMessage{
			Topic: "post-likes",
			Value: sarama.StringEncoder(jsonMessage),
		}

		_, _, err = producer.SendMessage(msg)
		if err != nil {
			log.Fatalln("Failed to produce message:", err)
			os.Exit(1)
		}
		log.Println("Message sent")
	}
}
