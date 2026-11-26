package ledger

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrImbalancedPosting   = errors.New("posting lines must sum to zero across debits and credits")
	ErrDuplicatePostingId  = errors.New("posting instruction already processed with identical idempotency key")
	ErrInvalidCurrency     = errors.New("currency mismatch across posting instruction lines")
	ErrNegativeAmount      = errors.New("posting line amount must be strictly positive")
	ErrAccountInactive     = errors.New("posting target account is inactive or frozen")
)

type Direction string

const (
	Debit  Direction = "DEBIT"
	Credit Direction = "CREDIT"
)

type PostingLine struct {
	AccountID string    `json:"account_id"`
	Direction Direction `json:"direction"`
	Amount    int64     `json:"amount"` // in minor units (cents/pence)
	Currency  string    `json:"currency"`
}

type PostingInstruction struct {
	ID             string        `json:"id"`
	IdempotencyKey string        `json:"idempotency_key"`
	Timestamp      time.Time     `json:"timestamp"`
	Lines          []PostingLine `json:"lines"`
}

type Gateway struct {
	mu        sync.RWMutex
	processed map[string]time.Time
}

func NewGateway() *Gateway {
	return &Gateway{
		processed: make(map[string]time.Time),
	}
}

func (g *Gateway) Process(inst PostingInstruction) error {
	if len(inst.Lines) < 2 {
		return errors.New("posting instruction must have at least 2 balancing lines")
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if _, exists := g.processed[inst.IdempotencyKey]; exists {
		return ErrDuplicatePostingId
	}

	var sumDebits, sumCredits int64
	var baseCurrency string

	for i, line := range inst.Lines {
		if line.Amount <= 0 {
			return ErrNegativeAmount
		}
		if i == 0 {
			baseCurrency = line.Currency
		} else if line.Currency != baseCurrency {
			return fmt.Errorf("%w: expected %s got %s", ErrInvalidCurrency, baseCurrency, line.Currency)
		}

		switch line.Direction {
		case Debit:
			sumDebits += line.Amount
		case Credit:
			sumCredits += line.Amount
		default:
			return fmt.Errorf("invalid direction: %s", line.Direction)
		}
	}

	if sumDebits != sumCredits {
		return fmt.Errorf("%w: debits=%d credits=%d", ErrImbalancedPosting, sumDebits, sumCredits)
	}

	g.processed[inst.IdempotencyKey] = time.Now()
	return nil
}
