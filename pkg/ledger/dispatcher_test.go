package ledger

import (
	"context"
	"testing"
	"time"
)

func TestDispatcher_BroadcastAndBalance(t *testing.T) {
	d := NewDispatcher(3)
	ch := d.Subscribe("sub-audit-service", 10)
	defer d.Unsubscribe("sub-audit-service")

	inst := PostingInstruction{
		ID:             "tx-2001",
		IdempotencyKey: "idem-2001",
		Timestamp:      time.Now(),
		Lines: []PostingLine{
			{AccountID: "acc-reserve", Direction: Debit, Amount: 75000, Currency: "SGD"},
			{AccountID: "acc-settlement", Direction: Credit, Amount: 75000, Currency: "SGD"},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := d.Dispatch(ctx, inst); err != nil {
		t.Fatalf("dispatch failed: %v", err)
	}

	select {
	case evt := <-ch:
		if evt.Payload.ID != "tx-2001" {
			t.Fatalf("expected tx-2001, got %s", evt.Payload.ID)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for event on channel")
	}

	if bal := d.GetBalance("acc-settlement"); bal != 75000 {
		t.Fatalf("expected credit balance 75000, got %d", bal)
	}
	if bal := d.GetBalance("acc-reserve"); bal != -75000 {
		t.Fatalf("expected debit balance -75000, got %d", bal)
	}
}

func TestDispatcher_DLQRecord(t *testing.T) {
	d := NewDispatcher(3)
	event := LedgerEvent{
		ID:        "evt-dead-1",
		Type:      EventPostingFailed,
		Timestamp: time.Now(),
	}

	d.RecordFailure(event, "downstream webhook 503 unavailable", 3)
	if count := d.DLQCount(); count != 1 {
		t.Fatalf("expected 1 DLQ entry, got %d", count)
	}
}

func BenchmarkDispatcher_Dispatch(b *testing.B) {
	d := NewDispatcher(3)
	ch := d.Subscribe("bench-sub", b.N)
	defer d.Unsubscribe("bench-sub")

	inst := PostingInstruction{
		ID:             "bench-tx",
		IdempotencyKey: "bench-key",
		Timestamp:      time.Now(),
		Lines: []PostingLine{
			{AccountID: "acc-a", Direction: Debit, Amount: 100, Currency: "SGD"},
			{AccountID: "acc-b", Direction: Credit, Amount: 100, Currency: "SGD"},
		},
	}

	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = d.Dispatch(ctx, inst)
		<-ch
	}
}
