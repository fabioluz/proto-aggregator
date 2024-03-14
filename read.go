package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func readMessages(app *App, chanBatches chan<- Batch) {
	currentBatch := make(Batch)

	sendBatch := func() {
		chanBatches <- currentBatch
		currentBatch = make(Batch)
	}

	for {
		msg, err := app.kafkaConsumer.ReadMessage(time.Second * 5)
		if err != nil {
			var kafkaErr kafka.Error
			if errors.As(err, &kafkaErr) && kafkaErr.Code() == kafka.ErrTimedOut {
				fmt.Println("Timeout, flushing queue")
				if len(currentBatch) > 0 {
					sendBatch()
				}
				continue
			}
			log.Fatalf("Error: %v\n", err)
		}

		// parse message
		jsonData := Log{}
		err = json.Unmarshal(msg.Value, &jsonData)
		if err != nil {
			log.Fatalf("Error reading message: %v", err)
		}

		// add message to the batch
		currentBatch[jsonData.LogId] = ConsumedLog{
			Log:            jsonData,
			TopicPartition: msg.TopicPartition,
		}

		if len(currentBatch) >= 500 {
			fmt.Println("Batch is full, sending to process")
			sendBatch()
		}
	}

}
