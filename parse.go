package main

import (
	"encoding/binary"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func parseBatch(batch Batch) ([]byte, error) {
	var batchData []byte
	for _, value := range batch {
		protoLog := &ProtoLog{
			LogId:     value.Log.LogId,
			Timestamp: timeToTimestamp(value.Log.Timestamp),
			Message:   value.Log.Message,
		}

		// serialize the protobuf message
		data, err := proto.Marshal(protoLog)
		if err != nil {
			return nil, fmt.Errorf("error serializing protobuf message: %w", err)
		}

		data = prependLittleEndianSize(data)
		batchData = append(batchData, data...)
	}

	return batchData, nil
}

func prependLittleEndianSize(data []byte) []byte {
	size := len(data)
	sizeBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(sizeBytes, uint32(size))
	return append(sizeBytes, data...)
}

func timeToTimestamp(t time.Time) *timestamppb.Timestamp {
	return &timestamppb.Timestamp{
		Seconds: int64(t.Unix()),
		Nanos:   int32(t.Nanosecond()),
	}
}
