package main

import (
	"testing"
	"time"
)

func TestGenerateTransaction(t *testing.T) {
	tx := generateTransaction(1)
	if tx.ID == "" {
		t.Error("ID must not be empty")
	}
	if tx.Amount <= 0 {
		t.Errorf("Amount must be positive, got %f", tx.Amount)
	}
	if tx.ShardID != 1 {
		t.Errorf("ShardID must be 1, got %d", tx.ShardID)
	}
	validStatuses := map[string]bool{"success": true, "failed": true, "pending": true}
	if !validStatuses[tx.Status] {
		t.Errorf("invalid status: %s", tx.Status)
	}
	validTypes := map[string]bool{"transfer": true, "payment": true, "withdrawal": true}
	if !validTypes[tx.Type] {
		t.Errorf("invalid type: %s", tx.Type)
	}
}

func TestTumblingWindow_Aggregate(t *testing.T) {
	out := make(chan WindowAggregate, 10)
	w := NewTumblingWindow(time.Second, out, 1)

	txs := []Transaction{
		{Amount: 100, Status: "success", Currency: "RUB", Timestamp: time.Now()},
		{Amount: 200, Status: "success", Currency: "RUB", Timestamp: time.Now()},
		{Amount: 300, Status: "failed", Currency: "USD", Timestamp: time.Now()},
	}
	for _, tx := range txs {
		w.Add(tx)
	}

	agg := w.aggregate(txs, time.Now())

	if agg.TotalCount != 3 {
		t.Errorf("expected 3, got %d", agg.TotalCount)
	}
	if agg.TotalAmount != 600 {
		t.Errorf("expected 600, got %f", agg.TotalAmount)
	}
	if agg.AvgAmount != 200 {
		t.Errorf("expected 200, got %f", agg.AvgAmount)
	}
	if agg.MinAmount != 100 {
		t.Errorf("expected 100, got %f", agg.MinAmount)
	}
	if agg.MaxAmount != 300 {
		t.Errorf("expected 300, got %f", agg.MaxAmount)
	}
	if agg.SuccessCount != 2 {
		t.Errorf("expected 2 success, got %d", agg.SuccessCount)
	}
	if agg.FailedCount != 1 {
		t.Errorf("expected 1 failed, got %d", agg.FailedCount)
	}
	if agg.Currency != "RUB" {
		t.Errorf("expected RUB dominant, got %s", agg.Currency)
	}
}

func TestTumblingWindow_EmptyFlush(t *testing.T) {
	out := make(chan WindowAggregate, 10)
	w := NewTumblingWindow(time.Second, out, 1)
	// Пустое окно не должно отправлять ничего
	w.flushAt(time.Now())
	if len(out) != 0 {
		t.Error("empty window should not produce output")
	}
}