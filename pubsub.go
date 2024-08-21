package main

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

func (ps *PubSub) Publish(channel, message string) {
	ps.RLock()
	defer ps.RUnlock()

	for _, sub := range ps.Subscribers[channel] {
        formattedMessage := fmt.Sprintf(`1) "message" \n 2) "%s" \n 3) "%s"`, channel, message)
        _, err := sub.Conn.Write([]byte(formattedMessage))
        if err != nil {
            log.Printf("Failed to send message to subscriber on channel %s: %v\n", channel, err)
            sub.Conn.Close()
            ps.removeSubscriber(channel, sub)
        }
    }
}

// removeSubscriber removes a subscriber from a channel.
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