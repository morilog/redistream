package redistream

import "time"

type Message struct {
	Payload   []byte
	GroupID   string
	Topic     string
	ID        string
	Timestamp time.Time
}
