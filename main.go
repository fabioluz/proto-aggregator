package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	sync "sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/confluentinc/confluent-kafka-go/kafka"
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

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  host,
		"group.id":           consumerGroup,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
	})
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
	ctx := context.Background()
	os.Setenv("STORAGE_EMULATOR_HOST", "http://localhost:4443")
	client, err := storage.NewClient(ctx, option.WithEndpoint("http://localhost:4443/storage/v1/"))

	if err != nil {
		log.Fatalf("Error connecting to google storage: %v", err)
		return nil
	}

	return client
}

// func writeMessage(writer io.Writer, message proto.Message) error {
// 	// Serialize the protobuf message
// 	data, err := proto.Marshal(message)
// 	if err != nil {
// 		return fmt.Errorf("failed to marshal message: %v", err)
// 	}

// 	// Write the length prefix
// 	length := len(data)
// 	if err := binary.Write(writer, binary.LittleEndian, int32(length)); err != nil {
// 		return fmt.Errorf("failed to write message length: %v", err)
// 	}

// 	// Write the serialized message
// 	if _, err := writer.Write(data); err != nil {
// 		return fmt.Errorf("failed to write message data: %v", err)
// 	}

// 	return nil
// }

// Open the file for reading
// file, err := os.Open("sd")
// if err != nil {
// 	log.Fatalf("failed to open file: %v", err)
// }
// defer file.Close()

// // Read messages until EOF
// for {
// 	// Read the next message from the file
// 	message, err := readMessage(file)
// 	if err == io.EOF {
// 		// No more messages to read, exit the loop
// 		break
// 	} else if err != nil {
// 		log.Fatalf("Failed to read message: %v", err)
// 	}

// 	// Process the message (e.g., print it)
// 	fmt.Printf("Read message: %+v\n", message)
// }

// func readMessage(file *os.File) (*ProtoLog, error) {
// 	// Read the length prefix of the next message
// 	var length int32
// 	err := binary.Read(file, binary.LittleEndian, &length)
// 	if err == io.EOF {
// 		return nil, io.EOF
// 	} else if err != nil {
// 		return nil, fmt.Errorf("failed to read message length: %v", err)
// 	}

// 	// Read the message data
// 	data := make([]byte, length)
// 	_, err = io.ReadFull(file, data)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to read message data: %v", err)
// 	}

// 	// Unmarshal the message data into a MyMessage struct
// 	message := &ProtoLog{}
// 	err = proto.Unmarshal(data, message)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to unmarshal message: %v", err)
// 	}

// 	return message, nil
// }

// Open the file for reading
// file, err := os.Open("output.pb")
// if err != nil {
//     log.Fatalf("failed to open file: %v", err)
// }
// defer file.Close()

// // Read messages until EOF
// for {
//     // Read the next message from the file
//     message, err := readMessage(file)
//     if err == io.EOF {
//         // No more messages to read, exit the loop
//         break
//     } else if err != nil {
//         log.Fatalf("Failed to read message: %v", err)
//     }

//     // Process the message (e.g., print it)
//     fmt.Printf("Read message: %+v\n", message)
// }

// file, err := os.Create("output.pb")
// if err != nil {
//     log.Fatalf("failed to create file: %v", err)
// }
// defer file.Close()

// Convert to protobuf
// protoLog := &ProtoLog{
// 	LogId:     jsonData.LogId,
// 	Timestamp: timeToTimestamp(jsonData.Timestamp),
// 	Message:   jsonData.Message,
// }

// // Write the message to the file with length prefix
// if err := writeMessage(file, protoLog); err != nil {
// 	log.Fatalf("failed to write message: %v", err)
// }

// // Manually commit the offset
// _, err = consumer.CommitMessage(msg)
// if err != nil {
// 	fmt.Printf("Failed to commit offset: %s\n", err)
// }
