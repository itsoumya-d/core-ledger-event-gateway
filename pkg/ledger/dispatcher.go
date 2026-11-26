package ledger

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type EventType string

const (
	EventPostingCommitted EventType = "POSTING_COMMITTED"
	EventPostingFailed    EventType = "POSTING_FAILED"
)

type LedgerEvent struct {
	ID        string             `json:"id"`
	Type      EventType          `json:"type"`
	Timestamp time.Time          `json:"timestamp"`
	Payload   PostingInstruction `json:"payload"`
}

type FailedEventEnvelope struct {
	Event    LedgerEvent `json:"event"`
	Reason   string      `json:"reason"`
	Attempt  int         `json:"attempt"`
	FailedAt time.Time   `json:"failed_at"`
}

type Dispatcher struct {
	mu          sync.RWMutex
	subscribers map[string]chan LedgerEvent
	balances    map[string]int64
	dlq         []FailedEventEnvelope
	maxRetries  int
}

func NewDispatcher(maxRetries int) *Dispatcher {
	return &Dispatcher{
		subscribers: make(map[string]chan LedgerEvent),
		balances:    make(map[string]int64),
		dlq:         make([]FailedEventEnvelope, 0),
		maxRetries:  maxRetries,
	}
}

func (d *Dispatcher) Subscribe(subscriberID string, bufferSize int) <-chan LedgerEvent {
	d.mu.Lock()
	defer d.mu.Unlock()

	ch := make(chan LedgerEvent, bufferSize)
	d.subscribers[subscriberID] = ch
	return ch
}

func (d *Dispatcher) Unsubscribe(subscriberID string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if ch, ok := d.subscribers[subscriberID]; ok {
		close(ch)
		delete(d.subscribers, subscriberID)
	}
}

func (d *Dispatcher) Dispatch(ctx context.Context, inst PostingInstruction) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	event := LedgerEvent{
		ID:        fmt.Sprintf("evt-%s", inst.ID),
		Type:      EventPostingCommitted,
		Timestamp: time.Now(),
		Payload:   inst,
	}

	for _, line := range inst.Lines {
		if line.Direction == Credit {
			d.balances[line.AccountID] += line.Amount
		} else {
			d.balances[line.AccountID] -= line.Amount
		}
	}

	for _, ch := range d.subscribers {
		select {
		case ch <- event:
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}

	return nil
}

func (d *Dispatcher) RecordFailure(event LedgerEvent, reason string, attempt int) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.dlq = append(d.dlq, FailedEventEnvelope{
		Event:    event,
		Reason:   reason,
		Attempt:  attempt,
		FailedAt: time.Now(),
	})
}

func (d *Dispatcher) GetBalance(accountID string) int64 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.balances[accountID]
}

func (d *Dispatcher) DLQCount() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.dlq)
}
