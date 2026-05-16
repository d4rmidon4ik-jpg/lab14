package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	etcdURL    = getEnv("ETCD_URL", "http://localhost:2379")
	shardIDStr = getEnv("SHARD_ID", "1")
	windowSec  = getEnv("WINDOW_SIZE", "10")
	outputDir  = getEnv("OUTPUT_DIR", "./data")
	arrowAddr  = getEnv("ARROW_ADDR", "0.0.0.0:8815")
)

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func registerShard(etcdClient *clientv3.Client, shardID int) error {
	key := fmt.Sprintf("/collectors/shard-%d", shardID)
	hostname, _ := os.Hostname()
	value := fmt.Sprintf(`{"shard_id":%d,"host":"%s","started_at":"%s"}`,
		shardID, hostname, time.Now().Format(time.RFC3339))

	// Регистрируемся с lease — при падении запись автоматически удалится
	lease, err := etcdClient.Grant(context.Background(), 30)
	if err != nil {
		return fmt.Errorf("etcd grant: %w", err)
	}

	_, err = etcdClient.Put(context.Background(), key, value,
		clientv3.WithLease(lease.ID))
	if err != nil {
		return fmt.Errorf("etcd put: %w", err)
	}

	// Keepalive горутина
	ch, err := etcdClient.KeepAlive(context.Background(), lease.ID)
	if err != nil {
		return fmt.Errorf("etcd keepalive: %w", err)
	}
	go func() {
		for range ch {
			// продолжаем keepalive
		}
	}()

	log.Printf("[etcd] registered shard %d at %s", shardID, key)
	return nil
}

func listShards(etcdClient *clientv3.Client) ([]string, error) {
	resp, err := etcdClient.Get(context.Background(), "/collectors/",
		clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	shards := make([]string, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		shards = append(shards, string(kv.Value))
	}
	return shards, nil
}

func main() {
	shardID, err := strconv.Atoi(shardIDStr)
	if err != nil {
		log.Fatalf("invalid SHARD_ID: %v", err)
	}
	windowSize, err := strconv.Atoi(windowSec)
	if err != nil {
		log.Fatalf("invalid WINDOW_SIZE: %v", err)
	}

	// Подключение к etcd (задание 1)
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdURL},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("etcd connect: %v", err)
	}
	defer etcdClient.Close()

	if err := registerShard(etcdClient, shardID); err != nil {
		log.Printf("WARNING: etcd registration failed: %v", err)
	}

	shards, _ := listShards(etcdClient)
	log.Printf("[etcd] active shards: %d", len(shards))

	// Graceful shutdown
	done := make(chan struct{})
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// Канал агрегатов из window
	aggCh := make(chan WindowAggregate, 100)

	// Arrow Flight сервер (задание 3)
	arrowSrv := NewArrowFlightServer()
	go func() {
		if err := startArrowServer(arrowAddr, arrowSrv); err != nil {
			log.Printf("Arrow server error: %v", err)
		}
	}()

	// Оконная агрегация (задание 2)
	window := NewTumblingWindow(
		time.Duration(windowSize)*time.Second,
		aggCh,
		shardID,
	)
	go window.Start(done)

	// Агрегаты → Arrow сервер + JSON файл
	var allAggs []WindowAggregate
	var aggMu sync.Mutex

	go func() {
		for agg := range aggCh {
			arrowSrv.AddAggregate(agg)
			aggMu.Lock()
			allAggs = append(allAggs, agg)
			aggMu.Unlock()

			// Пишем JSON для Python fallback
			writeAggToFile(agg, outputDir, shardID)
		}
	}()

	// Генерация транзакций (симуляция стриминга)
	txTicker := time.NewTicker(100 * time.Millisecond)
	processedCount := 0

	log.Printf("[collector] shard=%d started, window=%ds", shardID, windowSize)

	for {
		select {
		case <-quit:
			log.Printf("[collector] shutting down, processed %d transactions", processedCount)
			txTicker.Stop()
			close(done)
			// Ждём flush буфера
			time.Sleep(500 * time.Millisecond)
			// Сохраняем финальные данные
			saveFinal(allAggs, outputDir, shardID)
			log.Printf("[collector] graceful shutdown complete")
			return
		case <-txTicker.C:
			tx := generateTransaction(shardID)
			window.Add(tx)
			processedCount++
			if processedCount%100 == 0 {
				log.Printf("[collector] shard=%d processed=%d", shardID, processedCount)
			}
		}
	}
}

func writeAggToFile(agg WindowAggregate, dir string, shardID int) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Printf("mkdir error: %v", err)
		return
	}
	filename := fmt.Sprintf("%s/aggregates_shard%d.jsonl", dir, shardID)
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("open file error: %v", err)
		return
	}
	defer f.Close()

	data, err := json.Marshal(agg)
	if err != nil {
		log.Printf("marshal error: %v", err)
		return
	}
	if _, err := f.WriteString(string(data) + "\n"); err != nil {
		log.Printf("write error: %v", err)
	}
}

func saveFinal(aggs []WindowAggregate, dir string, shardID int) {
	if len(aggs) == 0 {
		return
	}
	filename := fmt.Sprintf("%s/final_shard%d.json", dir, shardID)
	data, _ := json.MarshalIndent(aggs, "", "  ")
	if err := os.WriteFile(filename, data, 0644); err != nil {
		log.Printf("save final error: %v", err)
	}
	log.Printf("[collector] saved %d aggregates to %s", len(aggs), filename)
}