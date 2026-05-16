package main

import (
	"fmt"
	"math/rand"
	"time"
)

// Transaction — финансовая транзакция
type Transaction struct {
	ID          string    `json:"id"`
	Timestamp   time.Time `json:"timestamp"`
	Amount      float64   `json:"amount"`
	Currency    string    `json:"currency"`
	AccountFrom string    `json:"account_from"`
	AccountTo   string    `json:"account_to"`
	Type        string    `json:"type"` // transfer, payment, withdrawal
	Status      string    `json:"status"` // success, failed, pending
	ShardID     int       `json:"shard_id"`
}

var (
	currencies = []string{"RUB", "USD", "EUR", "CNY"}
	txTypes    = []string{"transfer", "payment", "withdrawal"}
	statuses   = []string{"success", "success", "success", "failed", "pending"}
	rng        = rand.New(rand.NewSource(time.Now().UnixNano()))
)

// generateTransaction симулирует банковский стриминг транзакций
func generateTransaction(shardID int) Transaction {
	return Transaction{
		ID:          fmt.Sprintf("TXN-%d-%d", shardID, time.Now().UnixNano()),
		Timestamp:   time.Now(),
		Amount:      rng.Float64()*100000 + 10,
		Currency:    currencies[rng.Intn(len(currencies))],
		AccountFrom: fmt.Sprintf("ACC%08d", rng.Intn(99999999)),
		AccountTo:   fmt.Sprintf("ACC%08d", rng.Intn(99999999)),
		Type:        txTypes[rng.Intn(len(txTypes))],
		Status:      statuses[rng.Intn(len(statuses))],
		ShardID:     shardID,
	}
}