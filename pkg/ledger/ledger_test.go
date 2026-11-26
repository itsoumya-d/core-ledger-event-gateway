package ledger

import (
	"testing"
	"time"
)

func TestGateway_ProcessValidInstruction(t *testing.T) {
	gw := NewGateway()

	inst := PostingInstruction{
		ID:             "tx-1001",
		IdempotencyKey: "idem-key-abc-123",
		Timestamp:      time.Now(),
		Lines: []PostingLine{
			{AccountID: "acc-customer-checking", Direction: Debit, Amount: 50000, Currency: "SGD"},
			{AccountID: "acc-customer-savings", Direction: Credit, Amount: 50000, Currency: "SGD"},
		},
	}

	if err := gw.Process(inst); err != nil {
		t.Fatalf("expected successful processing, got: %v", err)
	}

	// Idempotency check
	if err := gw.Process(inst); err != ErrDuplicatePostingId {
		t.Fatalf("expected duplicate error, got: %v", err)
	}
}

func TestGateway_ProcessImbalancedInstruction(t *testing.T) {
	gw := NewGateway()

	inst := PostingInstruction{
		ID:             "tx-1002",
		IdempotencyKey: "idem-key-imbalanced",
		Timestamp:      time.Now(),
		Lines: []PostingLine{
			{AccountID: "acc-1", Direction: Debit, Amount: 50000, Currency: "SGD"},
			{AccountID: "acc-2", Direction: Credit, Amount: 40000, Currency: "SGD"},
		},
	}

	err := gw.Process(inst)
	if err == nil {
		t.Fatal("expected error on imbalanced posting, got nil")
	}
}
