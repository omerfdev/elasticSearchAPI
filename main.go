package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

func main() {
	cfg := elasticsearch.Config{
		Addresses: []string{
			"http://localhost:9200", // your Elasticsearch server URL
		},
	}
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("Error creating the client: %s", err)
	}

	indexName := "sample-index"

	// Create an index
	createIndex(es, indexName)

	// Index a document
	docID := "1"
	document := map[string]interface{}{
		"title": "Test Document",
		"body":  "This is a test document.",
	}
	indexDocument(es, indexName, docID, document)

	// Search for a document
	searchDocument(es, indexName, "Test")
}

func createIndex(es *elasticsearch.Client, indexName string) {
	res, err := es.Indices.Create(indexName)
	if err != nil {
		log.Fatalf("Error creating index: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Printf("Error creating index: %s", res.String())
	} else {
		log.Printf("Index created: %s", res.String())
	}
}

func indexDocument(es *elasticsearch.Client, indexName string, docID string, document map[string]interface{}) {
	data, err := json.Marshal(document)
	if err != nil {
		log.Fatalf("Error marshaling document: %s", err)
	}

	req := esapi.IndexRequest{
		Index:      indexName,
		DocumentID: docID,
		Body:       bytes.NewReader(data),
		Refresh:    "true",
	}

	res, err := req.Do(context.Background(), es)
	if err != nil {
		log.Fatalf("Error indexing document: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Printf("Error indexing document: %s", res.String())
	} else {
		log.Printf("Document indexed: %s", res.String())
	}
}

func searchDocument(es *elasticsearch.Client, indexName string, query string) {
	q := fmt.Sprintf(`{"query": {"match": {"title": "%s"}}}`, query)
	res, err := es.Search(
		es.Search.WithContext(context.Background()),
		es.Search.WithIndex(indexName),
		es.Search.WithBody(strings.NewReader(q)),
		es.Search.WithTrackTotalHits(true),
		es.Search.WithPretty(),
	)
	if err != nil {
		log.Fatalf("Error searching documents: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Printf("Error searching documents: %s", res.String())
	} else {
		var buf bytes.Buffer
		buf.ReadFrom(res.Body)
		fmt.Printf("Search result: %s\n", buf.String())
	}
}