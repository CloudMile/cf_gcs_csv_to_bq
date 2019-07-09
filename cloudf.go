package cloudf

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/functions/metadata"
)

// GCSEvent is the payload of a GCS event.
type GCSEvent struct {
	Bucket         string    `json:"bucket"`
	Name           string    `json:"name"`
	Metageneration string    `json:"metageneration"`
	ResourceState  string    `json:"resourceState"`
	TimeCreated    time.Time `json:"timeCreated"`
	Updated        time.Time `json:"updated"`
}

var projectID = os.Getenv("PROJECT_ID")
var datasetName = os.Getenv("DATASET_NAME")
var tableName = os.Getenv("TABLE_NAME")
var partitioningField = os.Getenv("PARTITIONING_FIELD")
var client *bigquery.Client

func init() {
	var err error
	client, err = bigquery.NewClient(context.Background(), projectID)
	if err != nil {
		log.Fatalf("bigquery.NewClient: %v", err)
	}
}

// GCSToBigQuery is import csv to BQ with GCS event.
func GCSToBigQuery(ctx context.Context, e GCSEvent) error {
	meta, err := metadata.FromContext(ctx)
	if err != nil {
		return fmt.Errorf("metadata.FromContext: %v", err)
	}
	log.Printf("Event ID: %v\n", meta.EventID)
	log.Printf("Event type: %v\n", meta.EventType)
	log.Printf("Bucket: %v\n", e.Bucket)
	log.Printf("File: %v\n", e.Name)

	gcsURI := "gs://" + e.Bucket + "/" + e.Name
	gcsRef := bigquery.NewGCSReference(gcsURI)
	gcsRef.AllowJaggedRows = true
	gcsRef.FileConfig.AutoDetect = true
	gcsRef.FileConfig.SourceFormat = bigquery.CSV

	myDataset := client.DatasetInProject(projectID, datasetName)
	loader := myDataset.Table(tableName).LoaderFrom(gcsRef)
	loader.CreateDisposition = bigquery.CreateIfNeeded
	loader.WriteDisposition = bigquery.WriteTruncate

	// If you don't need partitioning, comment out.
	timePartitioning := bigquery.TimePartitioning{Field: partitioningField}
	loader.TimePartitioning = &timePartitioning

	_, err = loader.Run(ctx)
	if err != nil {
		return fmt.Errorf("bq load failed: %v", err)
	}
	return nil
}
