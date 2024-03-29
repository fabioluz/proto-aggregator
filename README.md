# Proto Aggregator

Proto Aggregator is a tool that solves the problem of saving a copy of all Kafka messages in long-term storage. It achieves this by compiling the messages using Protocol Buffers (protobuf) and uploading them to an upstream service in batches.

## Running the project

You can run this project by running the docker compose file.

```
docker compose up
```

This will spin up Kafka, PostgreSQL, and a Mocked Google Cloud Storage server.

To see the application in action, you can produce kafka messages by running:

```
cat examples/messages.jsonl | docker exec -i kafka kafka-console-producer.sh --producer.config /opt/bitnami/kafka/config/producer.properties --bootstrap-server 127.0.0.1:9094 --topic logs
```

By inspecting the logs, you should see the application aggregating the messages into just one file.
You can download the file at:
```
curl "https://localhost:4443/storage/v1/b/logs/o/log.pb?alt=media" -o "all-logs.pb"
```

## How it Works

Proto Aggregator efficiently handles Kafka messages by utilizing Go routines. It continuously reads messages from Kafka and accumulates them into a batch. When the batch is full or no messages are received for a specified period, it sends the batch to a channel. Another Go routine processes this channel, removing potential duplicates, serializing the messages using protobuf, and appending the compiled data to a file in Google Cloud Storage.

### Removing Duplicates

When streaming from Kafka, it is possible that you read the same message multiple times. To solve that problem Proto Aggregator uses a PostgreSQL transactions to make sure that the same message can only be saved once, even if you are using multiple aggregator instances. In the project's case, the messages are `logs` generated by the system, which should be saved in a long-term storage for future analysis.

It has a table `processed_logs` with one `log_id` column.

```
CREATE TABLE IF NOT EXISTS processed_logs (log_id UUID);
CREATE INDEX IF NOT EXISTS processed_logs_log_id_idx ON processed_logs USING HASH (log_id);
```

When a batch is received by the `processBatches` function, an SQL Transaction is opened.

The system takes an `EXCLUSIVE LOCK` to `processed_logs` table, making sure that other instances will have to wait for this transaction before saving their data.

Now, we need to check if the new batch does not contain a `log` that already exists in `processed_logs` table. A simple query such as

```
SELECT log_id FROM processed_logs WHERE log_in IN (<HUGE LIST OF IDS>)
```

would be enough to give a list of duplicated logs, but that is very inefficient. If the list inside the `IN` clause is too big, the query would be too slow. The bigger the batch, slower the query.

As a better approach, we create a temporary table called `temp_processed_logs`.

```
CREATE TEMP TABLE temp_logs (log_id UUID) ON COMMIT DROP;
CREATE INDEX temp_logs_log_id_idx ON temp_logs USING HASH (log_id);
```

Now, we can use Postgre `COPY` command to efficiently insert all the `log_id`s from the batch into the temporary table.
Then, we just need to join both tables to extract duplicated `log_id`s.

```
SELECT t.log_id
FROM temp_logs t
INNER JOIN processed_logs p ON t.log_id = p.log_id
```

This will give us all existing logs, which should be removed from the batch if there is any.
We can now insert all the new logs into the final `processed_logs` table using `COPY` command. You can see this logic in action by inspecting the `deduplicateBatch` function.

### Serializing messages with Protobuf

Protobuf is great for serializing strucutred objects into a binary format. However, it doesn't come with delimiters, which means that if you add many objects in the same file, you don't know when the objects start and end. This makes the deserializing process much harder, if not impossible. 

In order to solve that, we can use custom delimiters to tell us when each object ends. This is a common technique used in many projects out there and it is perfect for this use case. All we need to do is to add the size of the message (in bytes) before each message, so our final compiled file will look like this:

```
size | message | size | message | size | message ...
```

You can see how that is done in the code by checking the implementation of `parseBatch` and `prependLittleEndianSize`.

### Append messages to Google Cloud Storage

Now that we have our messages compiled, we want to append that into our main in file GCS.
All we need to do is:
- Upload a temporary file with the new data using the [Upload API](https://cloud.google.com/storage/docs/uploading-objects)
- Compose the temporary file with the main file using the [Compose API](https://cloud.google.com/storage/docs/composing-objects)
- Deleting the temporary file using [Delete API](https://cloud.google.com/storage/docs/deleting-objects)

## Deserializing the logs

To deserilize the logs from the final file, you can download the file as describe above in this document, then use the following script read the file:

```
func main() {
    // Open the file for reading
    file, err := os.Open("your-downloaded-file.pb")
    if err != nil {
        log.Fatalf("failed to open file: %v", err)
    }
    defer file.Close()

    // Read messages until EOF
    for {
        // Read the next message from the file
        message, err := readMessage(file)
        if err == io.EOF {
            // No more messages to read, exit the loop
            break
        } else if err != nil {
            log.Fatalf("Failed to read message: %v", err)
        }

        // Process the message (e.g., print it)
        fmt.Printf("Read message: %+v\n", message)
    }
}

func readMessage(file *os.File) (*ProtoLog, error) {
	// Read the length prefix of the next message
	var length int32
	err := binary.Read(file, binary.LittleEndian, &length)
	if err == io.EOF {
		return nil, io.EOF
	} else if err != nil {
		return nil, fmt.Errorf("failed to read message length: %v", err)
	}

	// Read the message data
	data := make([]byte, length)
	_, err = io.ReadFull(file, data)
	if err != nil {
		return nil, fmt.Errorf("failed to read message data: %v", err)
	}

	// Unmarshal the message data into a MyMessage struct
	message := &ProtoLog{}
	err = proto.Unmarshal(data, message)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal message: %v", err)
	}

	return message, nil
}

```





