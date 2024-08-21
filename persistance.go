package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

type Persistence struct {
	filePath string
}

func NewPersistence(filePath string) *Persistence {
	return &Persistence{filePath: filePath}
}

// SAVE command: saves the current database to disk
func (p *Persistence) Save(kvstore *KeyValueStore) error {
	data, err := json.Marshal(kvstore)
	if err != nil {
		return fmt.Errorf("failed to serialize database: %v", err)
	}

	// Backup existing file
	if _, err := os.Stat(p.filePath); err == nil {
		err = os.Rename(p.filePath, p.filePath+".bak")
		if err != nil {
			return fmt.Errorf("failed to create backup: %v", err)
		}
	}

	err = os.WriteFile(p.filePath, data, 0644)
	if err != nil {
		return fmt.Errorf("failed to save database: %v", err)
	}
	return nil
}

// BGSAVE command: saves the database to disk in the background
func (p *Persistence) Bgsave(kvstore *KeyValueStore) {
	go func() {
		time.Sleep(2 * time.Second) // Simulate time taken to save
		err := p.Save(kvstore)
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Println("Database saved in the background.")
		}
	}()
}

// Load loads the database from disk
func (p *Persistence) Load(kvstore *KeyValueStore) error {
	data, err := os.ReadFile(p.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			// File does not exist, return nil to indicate no data to load
			return nil
		}
		return fmt.Errorf("failed to load database: %v", err)
	}
	err = json.Unmarshal(data, kvstore)
	if err != nil {
		return fmt.Errorf("failed to deserialize database: %v", err)
	}
	return nil
}