package storage

import (
	"sync"
	"time"
)

type KeyValueStore struct {
	Strings               map[string]string
    Lists                 map[string][]string
    Hashes                map[string]map[string]string
    Sets                  map[string]map[string]struct{}
    SortedSets            map[string]map[string]float64
    Expirations           map[string]time.Time
	sync.RWMutex
}

func NewKeyValueStore() *KeyValueStore {
	return &KeyValueStore{
		Strings:               make(map[string]string),
        Lists:                 make(map[string][]string),
        Hashes:                make(map[string]map[string]string),
        Sets:                  make(map[string]map[string]struct{}),
        SortedSets:            make(map[string]map[string]float64),
        Expirations:           make(map[string]time.Time),
	}
}