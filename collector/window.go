package main

import (
	"log"
	"sync"
	"time"
)

// WindowAggregate — результат агрегации за окно
type WindowAggregate struct {
	WindowStart  time.Time `json:"window_start"`
	WindowEnd    time.Time `json:"window_end"`
	ShardID      int       `json:"shard_id"`
	TotalCount   int       `json:"total_count"`
	TotalAmount  float64   `json:"total_amount"`
	AvgAmount    float64   `json:"avg_amount"`
	MinAmount    float64   `json:"min_amount"`
	MaxAmount    float64   `json:"max_amount"`
	SuccessCount int       `json:"success_count"`
	FailedCount  int       `json:"failed_count"`
	Currency     string    `json:"currency"` // доминирующая валюта
}

// TumblingWindow реализует tumbling window агрегацию
type TumblingWindow struct {
	mu           sync.Mutex
	windowSize   time.Duration
	transactions []Transaction
	outCh        chan<- WindowAggregate
	shardID      int
}

func NewTumblingWindow(size time.Duration, out chan<- WindowAggregate, shardID int) *TumblingWindow {
	return &TumblingWindow{
		windowSize: size,
		outCh:      out,
		shardID:    shardID,
	}
}

func (w *TumblingWindow) Add(tx Transaction) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.transactions = append(w.transactions, tx)
}

func (w *TumblingWindow) Start(done <-chan struct{}) {
	ticker := time.NewTicker(w.windowSize)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			w.flush() // сбрасываем буфер при shutdown
			return
		case t := <-ticker.C:
			w.flushAt(t)
		}
	}
}

func (w *TumblingWindow) flushAt(windowEnd time.Time) {
	w.mu.Lock()
	txs := w.transactions
	w.transactions = nil
	w.mu.Unlock()

	if len(txs) == 0 {
		return
	}

	agg := w.aggregate(txs, windowEnd)
	w.outCh <- agg
	log.Printf("[window] shard=%d flushed %d txs, total=%.2f avg=%.2f",
		w.shardID, agg.TotalCount, agg.TotalAmount, agg.AvgAmount)
}

func (w *TumblingWindow) flush() {
	w.flushAt(time.Now())
}

func (w *TumblingWindow) aggregate(txs []Transaction, windowEnd time.Time) WindowAggregate {
	if len(txs) == 0 {
		return WindowAggregate{}
	}

	total := 0.0
	minA := txs[0].Amount
	maxA := txs[0].Amount
	success := 0
	failed := 0
	currencies := map[string]int{}

	for _, tx := range txs {
		total += tx.Amount
		if tx.Amount < minA {
			minA = tx.Amount
		}
		if tx.Amount > maxA {
			maxA = tx.Amount
		}
		if tx.Status == "success" {
			success++
		} else if tx.Status == "failed" {
			failed++
		}
		currencies[tx.Currency]++
	}

	// Доминирующая валюта
	domCurrency := ""
	maxCount := 0
	for c, cnt := range currencies {
		if cnt > maxCount {
			maxCount = cnt
			domCurrency = c
		}
	}

	return WindowAggregate{
		WindowStart:  txs[0].Timestamp,
		WindowEnd:    windowEnd,
		ShardID:      w.shardID,
		TotalCount:   len(txs),
		TotalAmount:  total,
		AvgAmount:    total / float64(len(txs)),
		MinAmount:    minA,
		MaxAmount:    maxA,
		SuccessCount: success,
		FailedCount:  failed,
		Currency:     domCurrency,
	}
}