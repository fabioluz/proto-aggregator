package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"google.golang.org/api/option"
)

type Log struct {
	LogId     string    `json:"logId"`
	Timestamp time.Time `json:"timestamp"`
	Message   string    `json:"message"`
}

type ConsumedLog struct {
	Log            Log
	TopicPartition kafka.TopicPartition
}

type Batch map[string]ConsumedLog

type App struct {
	kafkaConsumer *kafka.Consumer
	sqlConnection *sql.DB
	storageClient *storage.Client
	storageBucket string
}

func main() {
	fmt.Println("Initializing")
	time.Sleep(10 * time.Second)

	kafkaConsumer := createKafkaConsumer()
	sqlConnection := createSqlConnection()
	storageClient := createStorageClient()
	storageBucket := getEnvOrDefault("STORAGE_BUCKET", "logs")

	app := &App{
		kafkaConsumer,
		sqlConnection,
		storageClient,
		storageBucket,
	}

	var wg sync.WaitGroup
	wg.Add(1)

	chanBatches := make(chan Batch)
	go readMessages(app, chanBatches)
	go processBatches(app, chanBatches)

	wg.Wait()
}

func getEnvOrDefault(varName string, defaultVar string) string {
	value, exists := os.LookupEnv(varName)
	if !exists {
		return defaultVar
	}

	return value
}

func createSqlConnection() *sql.DB {
	user := getEnvOrDefault("DB_USER", "my_user")
	password := getEnvOrDefault("DB_PASSWORD", "password123")
	dbName := getEnvOrDefault("DB_NAME", "proto_parser_db")
	host := getEnvOrDefault("DB_HOST", "localhost")
	port := getEnvOrDefault("DB_PORT", "5432")

	connectionString := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=disable",
		user,
		password,
		host,
		port,
		dbName,
	)

	// create connection
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		log.Fatalf("Error opening database connection: %v", err)
		return nil
	}

	// check if the connection is successful
	err = db.Ping()
	if err != nil {
		log.Fatalf("Error connecting to database: %v", err)
		return nil
	}

	// add processed logs table
	createTableQuery := `
		CREATE TABLE IF NOT EXISTS processed_logs (log_id UUID);
		CREATE INDEX IF NOT EXISTS processed_logs_log_id_idx ON processed_logs USING HASH (log_id);
	`
	_, err = db.Exec(createTableQuery)
	if err != nil {
		log.Fatalf("Error creating processed_logs table: %v", err)
		return nil
	}

	return db
}

func createKafkaConsumer() *kafka.Consumer {
	host := getEnvOrDefault("KAFKA_HOST", "localhost:9094")
	consumerGroup := getEnvOrDefault("KAFKA_CONSUMER_GROUP", "group-1")

	// create topic
	adminConfig := &kafka.ConfigMap{
		"bootstrap.servers": host,
	}
	admin, err := kafka.NewAdminClient(adminConfig)
	if err != nil {
		log.Fatalf("Failed to create admin client: %v\n", err)
	}

	defer admin.Close()

	topicConfig := kafka.TopicSpecification{
		Topic:             "logs",
		NumPartitions:     1,
		ReplicationFactor: 1,
		Config:            map[string]string{},
	}

	_, err = admin.CreateTopics(context.Background(), []kafka.TopicSpecification{topicConfig})
	if err != nil {
		// check if the error indicates that the topic already exists
		log.Fatalf("Failed to create topic: %s\n", err)
	}

	// create consumer
	consumerConfig := &kafka.ConfigMap{
		"bootstrap.servers":  host,
		"group.id":           consumerGroup,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
	}

	consumer, err := kafka.NewConsumer(consumerConfig)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v\n", err)
	}

	err = consumer.SubscribeTopics([]string{"logs"}, nil)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v\n", err)
	}

	return consumer
}

func createStorageClient() *storage.Client {
	endpoint := getEnvOrDefault("GCS_ENDPOINT", "http://localhost:4443/storage/v1/")

	// make sure storage emulator host is set
	// it is required by storage mock
	os.Setenv("STORAGE_EMULATOR_HOST", endpoint)

	client, err := storage.NewClient(context.Background(), option.WithEndpoint(endpoint))
	if err != nil {
		log.Fatalf("Error connecting to google storage: %v", err)
		return nil
	}

	return client
}
