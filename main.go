package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	_ "github.com/lib/pq"
)

// Database connection details
const (
	DB_HOST     = "localhost"
	DB_PORT     = 5432
	DB_USER     = "__POSTGRESS_USER__"
	DB_PASSWORD = "__POSTGRESS_PASSWORD__"
	DB_NAME     = "__POSTGRESS_DB_NAME__"
)

// API URL and key
const (
	API_URL = "https://api-mocha.celenium.io/v1/rollup/10/blobs?limit=1&offset=0&sort=desc&sort_by=time"
	API_KEY = "__YOUR_API_KEY_HERE__"
)

// Blob represents the structure of each blob from the API response
type Blob struct {
	ID          int    `json:"id"`
	Commitment  string `json:"commitment"`
	Size        int    `json:"size"`
	Height      int    `json:"height"`
	Time        string `json:"time"`
	Signer      string `json:"signer"`
	ContentType string `json:"content_type"`
	Namespace   struct {
		NamespaceID string `json:"namespace_id"`
	} `json:"namespace"`
	Tx struct {
		ID       int    `json:"id"`
		Height   int    `json:"height"`
		Position int    `json:"position"`
		Hash     string `json:"hash"`
	} `json:"tx"`
}

// FetchBlobs makes a GET request to the API and retrieves the blob data
func FetchBlobs() ([]Blob, error) {
	client := &http.Client{}
	req, err := http.NewRequest("GET", API_URL, nil)
	if err != nil {
		return nil, err
	}

	// Add API key to request header
	req.Header.Add("apikey", API_KEY)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch data: %d %s", resp.StatusCode, resp.Status)
	}

	var blobs []Blob
	err = json.NewDecoder(resp.Body).Decode(&blobs)
	if err != nil {
		return nil, err
	}

	return blobs, nil
}

// CreateTableIfNotExists creates the blobs table if it doesn't already exist
func CreateTableIfNotExists(db *sql.DB) error {
	query := `
		CREATE TABLE IF NOT EXISTS blobs (
			id INT PRIMARY KEY,
			commitment TEXT,
			size INT,
			height INT,
			time TIMESTAMP,
			signer TEXT,
			content_type TEXT,
			namespace_id TEXT,
			tx_id INT,
			tx_height INT,
			tx_position INT,
			tx_hash TEXT
		)
	`
	_, err := db.Exec(query)
	if err != nil {
		return err
	}
	log.Println("Table 'blobs' ensured.")
	return nil
}

// SaveBlobToPostgres saves the blob data to PostgreSQL
func SaveBlobToPostgres(db *sql.DB, blob Blob) error {
	query := `
		INSERT INTO blobs (id, commitment, size, height, time, signer, content_type, namespace_id, tx_id, tx_height, tx_position, tx_hash)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT (id) DO NOTHING
	`

	_, err := db.Exec(query, blob.ID, blob.Commitment, blob.Size, blob.Height, blob.Time, blob.Signer,
		blob.ContentType, blob.Namespace.NamespaceID, blob.Tx.ID, blob.Tx.Height, blob.Tx.Position, blob.Tx.Hash)

	if err != nil {
		return err
	}

	return nil
}

// ConnectPostgres establishes the connection to the PostgreSQL database
func ConnectPostgres() (*sql.DB, error) {
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	return db, nil
}

func main() {
	db, err := ConnectPostgres()
	if err != nil {
		log.Fatalf("Could not connect to PostgreSQL: %v", err)
	}
	defer db.Close()

	err = CreateTableIfNotExists(db)
	if err != nil {
		log.Fatalf("Could not create table: %v", err)
	}

	// Run an infinite loop to fetch data every 12 seconds
	for {
		// Fetch blobs from API
		blobs, err := FetchBlobs()
		if err != nil {
			log.Printf("Error fetching blobs: %v", err)
		} else if len(blobs) > 0 {
			// Save the first blob to the database
			err = SaveBlobToPostgres(db, blobs[0])
			if err != nil {
				log.Printf("Error saving blob to PostgreSQL: %v", err)
			} else {
				log.Printf("Successfully saved blob ID %d", blobs[0].ID)
			}
		}

		// Sleep for 12 seconds before fetching again
		time.Sleep(12 * time.Second)
	}
}
