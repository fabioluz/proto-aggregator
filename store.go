package main

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/storage"
)

func uploadTempObject(client *storage.Client, bucket, object string, data []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*50)
	defer cancel()

	// upload an object with storage.Writer.
	wc := client.Bucket(bucket).Object(object).NewWriter(ctx)
	wc.ChunkSize = 0 // note retries are not supported for chunk size 0.

	// buffer := bytes.NewBuffer(data)

	if _, err := wc.Write(data); err != nil {
		return fmt.Errorf("error writing data to gcs: %w", err)
	}

	// if _, err := io.Copy(wc, buffer); err != nil {
	// 	return fmt.Errorf("error uploading file: %w", err)
	// }

	// data can continue to be added to the file until the writer is closed.
	if err := wc.Close(); err != nil {
		return fmt.Errorf("error closing writer: %w", err)
	}

	fmt.Println("Temp file uploaded successfully")
	return nil
}

func composeObjects(client *storage.Client, bucket, tempObject, destObject string) error {
	// append time object in the final object
	tempObj := client.Bucket(bucket).Object(tempObject)
	destObj := client.Bucket(bucket).Object(destObject)

	// compose temp object with existing object
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	_, err := destObj.ComposerFrom(destObj, tempObj).Run(ctx)
	if err != nil {
		return fmt.Errorf("error when composing objects: %w", err)
	}

	// delete temp object
	if err = tempObj.Delete(ctx); err != nil {
		return fmt.Errorf("error when deleting objects: %w", err)
	}

	fmt.Printf("New composite object %v was created by combining %v and %v\n", destObj, destObj, tempObj)
	return nil
}

func storeBatch(app *App, data []byte) error {
	bucket := app.storageBucket
	destObject := "log.pb"
	tempObject := "temp-log-2.pb"

	// insert temp object
	err := uploadTempObject(app.storageClient, bucket, tempObject, data)
	if err != nil {
		return err
	}

	return composeObjects(app.storageClient, bucket, tempObject, destObject)
}
