package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func readMessages(app *App, chanBatches chan<- Batch) {
	currentBatch := make(Batch)

	batchSize, err := strconv.Atoi(getEnvOrDefault("BATCH_SIZE", "500"))
	if err != nil {
		log.Fatalf("Error reading batch size: %v", err)
	}

	timeout, err := time.ParseDuration(getEnvOrDefault("TIMEOUT", "15s"))
	if err != nil {
		log.Fatalf("Error reading timeout: %v", err)
	}

	sendBatch := func() {
		chanBatches <- currentBatch
		currentBatch = make(Batch)
	}

	for {
		msg, err := app.kafkaConsumer.ReadMessage(timeout)
		if err != nil {
			var kafkaErr kafka.Error
			if errors.As(err, &kafkaErr) && kafkaErr.Code() == kafka.ErrTimedOut {
				fmt.Println("Timeout, committing batch")
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

		if len(currentBatch) >= batchSize {
			fmt.Println("Batch is full, sending to process")
			sendBatch()
		}
	}

}
