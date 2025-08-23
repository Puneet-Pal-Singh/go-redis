package pubsub

import (
	"log"
	"net"
	"sync"
	"fmt"
)

type Subscriber struct {
	Channel string
	Conn    net.Conn
}

type PubSub struct {
	sync.RWMutex
	Subscribers map[string][]Subscriber
}

func NewPubSub() *PubSub {
	return &PubSub{
		Subscribers: make(map[string][]Subscriber),
	}
}

func (ps *PubSub) Subscribe(channel string, conn net.Conn) {
	ps.Lock()
	defer ps.Unlock()
	subscriber := Subscriber{Channel: channel, Conn: conn}
	ps.Subscribers[channel] = append(ps.Subscribers[channel], subscriber)
}

func (ps *PubSub) Unsubscribe(channel string, conn net.Conn) {
	ps.Lock()
	defer ps.Unlock()
	subscriber := Subscriber{Channel: channel, Conn: conn}
	ps.removeSubscriber(channel, subscriber)
}

func (ps *PubSub) Publish(channel, message string) int {
	ps.RLock()

	subscribers, ok := ps.Subscribers[channel]
	if !ok {
		ps.RUnlock()
		return 0
	}

	// RESP array: ["message", channel, message]
	payload := fmt.Sprintf("*3\r\n$7\r\nmessage\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(channel), channel, len(message), message)

	var failedSubs []Subscriber
	sentCount := 0
	for _, sub := range subscribers {
		_, err := sub.Conn.Write([]byte(payload))
		if err != nil {
			log.Printf("Failed to send message to subscriber on channel %s: %v\n", channel, err)
			failedSubs = append(failedSubs, sub)
			sub.Conn.Close()
		} else {
			sentCount++
		}
	}
	ps.RUnlock() // Release read lock before acquiring write lock for removals

	if len(failedSubs) > 0 {
		ps.Lock()
		for _, sub := range failedSubs {
			ps.removeSubscriber(sub.Channel, sub)
		}
		ps.Unlock()
	}

	return sentCount
}

// removeSubscriber removes a subscriber from a channel.
// It assumes the caller holds a write lock.
func (ps *PubSub) removeSubscriber(channel string, sub Subscriber) {
    if subscribers, ok := ps.Subscribers[channel]; ok {
        for i, subscriber := range subscribers {
            if subscriber.Conn == sub.Conn {
                ps.Subscribers[channel] = append(subscribers[:i], subscribers[i+1:]...)
                break
            }
        }
        if len(ps.Subscribers[channel]) == 0 {
            delete(ps.Subscribers, channel)
        }
    }
}