//go:build ignore

package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
)

// WindowAggregate дублируем здесь для standalone запуска
type WindowAggregate struct {
	WindowStart  string  `json:"window_start"`
	WindowEnd    string  `json:"window_end"`
	ShardID      int     `json:"shard_id"`
	TotalCount   int     `json:"total_count"`
	TotalAmount  float64 `json:"total_amount"`
	AvgAmount    float64 `json:"avg_amount"`
	MinAmount    float64 `json:"min_amount"`
	MaxAmount    float64 `json:"max_amount"`
	SuccessCount int     `json:"success_count"`
	FailedCount  int     `json:"failed_count"`
	Currency     string  `json:"currency"`
}

func main() {
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}

	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Fatalf("NATS connect error: %v", err)
	}
	defer nc.Close()
	log.Printf("[nats-producer] Connected to %s", natsURL)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	published := 0
	for {
		select {
		case <-quit:
			log.Printf("[nats-producer] Shutting down, published %d messages", published)
			return
		case <-ticker.C:
			// Публикуем агрегат каждые 10 секунд
			agg := WindowAggregate{
				WindowStart:  time.Now().Add(-10 * time.Second).Format(time.RFC3339),
				WindowEnd:    time.Now().Format(time.RFC3339),
				ShardID:      1,
				TotalCount:   100,
				TotalAmount:  500000.0 + float64(published*1000),
				AvgAmount:    5000.0,
				MinAmount:    100.0,
				MaxAmount:    50000.0,
				SuccessCount: 85,
				FailedCount:  15,
				Currency:     "RUB",
			}
			data, err := json.Marshal(agg)
			if err != nil {
				log.Printf("[nats-producer] marshal error: %v", err)
				continue
			}
			if err := nc.Publish("transactions.aggregates", data); err != nil {
				log.Printf("[nats-producer] publish error: %v", err)
				continue
			}
			published++
			log.Printf("[nats-producer] published aggregate #%d to transactions.aggregates", published)
		}
	}
}