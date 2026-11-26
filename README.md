# Core Ledger Event Gateway

High-throughput, idempotent event gateway and posting invariant validation runtime for core banking ledgers.

## Architecture
- **Posting Invariant Validation**: Enforces zero-sum double-entry accounting (`sum(debits) == sum(credits)`), strict ISO-4217 currency matching, and non-negative asset/liability balances.
- **Idempotency & Deduplication**: High-speed in-memory LRU deduplication with deterministic idempotency keys and out-of-order sequence rejection.
- **Resilient Dispatch**: Exponential backoff retry envelope with dead-letter queue (DLQ) routing for downstream client webhook and gRPC listeners.

## Quickstart
```bash
go test -v -race ./...
go test -bench=. ./...
```
