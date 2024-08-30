package redistream

import "time"

// Message represents each redis stream item
type Message struct {
	Payload   []byte
	GroupID   string
	Topic     string
	ID        string
	Timestamp time.Time
}
