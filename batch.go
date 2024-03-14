package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func processBatches(app *App, chanBatches <-chan Batch) {
	for batch := range chanBatches {
		fmt.Printf("Processing batch: %d items\n", len(batch))

		// store partitions for commiting
		topicPartitions := make([]kafka.TopicPartition, len(batch))
		for _, value := range batch {
			topicPartitions = append(topicPartitions, value.TopicPartition)
		}

		// use db to deduplicate items
		err := withTransaction(app.sqlConnection, func(tx *sql.Tx) error {
			batch, err := deduplicateBatch(tx, batch)
			if err != nil {
				return err
			}

			parsedBatch, err := parseBatch(batch)
			if err != nil {
				return err
			}

			if err = storeBatch(app, parsedBatch); err != nil {
				return err
			}

			app.kafkaConsumer.CommitOffsets(topicPartitions)
			fmt.Println("Commiting batch")
			return nil
		})

		if err != nil {
			log.Fatal(err)
		}

		time.Sleep(5 * time.Second)
	}
}
