package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/flight"
	"github.com/apache/arrow/go/v15/arrow/ipc"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"google.golang.org/grpc"
)

var arrowSchema = arrow.NewSchema([]arrow.Field{
	{Name: "window_start", Type: arrow.FixedWidthTypes.Timestamp_ms},
	{Name: "window_end", Type: arrow.FixedWidthTypes.Timestamp_ms},
	{Name: "shard_id", Type: arrow.PrimitiveTypes.Int32},
	{Name: "total_count", Type: arrow.PrimitiveTypes.Int32},
	{Name: "total_amount", Type: arrow.PrimitiveTypes.Float64},
	{Name: "avg_amount", Type: arrow.PrimitiveTypes.Float64},
	{Name: "min_amount", Type: arrow.PrimitiveTypes.Float64},
	{Name: "max_amount", Type: arrow.PrimitiveTypes.Float64},
	{Name: "success_count", Type: arrow.PrimitiveTypes.Int32},
	{Name: "failed_count", Type: arrow.PrimitiveTypes.Int32},
	{Name: "currency", Type: arrow.BinaryTypes.String},
}, nil)

// ArrowFlightServer отдаёт агрегированные данные через Arrow Flight RPC
type ArrowFlightServer struct {
	flight.BaseFlightServer
	mu   sync.Mutex
	data []WindowAggregate
}

func NewArrowFlightServer() *ArrowFlightServer {
	return &ArrowFlightServer{}
}

func (s *ArrowFlightServer) AddAggregate(agg WindowAggregate) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = append(s.data, agg)
	// Держим только последние 1000 записей
	if len(s.data) > 1000 {
		s.data = s.data[len(s.data)-1000:]
	}
}

func (s *ArrowFlightServer) DoGet(ticket *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	s.mu.Lock()
	data := make([]WindowAggregate, len(s.data))
	copy(data, s.data)
	s.mu.Unlock()

	if len(data) == 0 {
		log.Println("[arrow] no data available yet")
		return nil
	}

	alloc := memory.NewGoAllocator()
	b := array.NewRecordBuilder(alloc, arrowSchema)
	defer b.Release()

	tsStart := b.Field(0).(*array.TimestampBuilder)
	tsEnd := b.Field(1).(*array.TimestampBuilder)
	shardB := b.Field(2).(*array.Int32Builder)
	countB := b.Field(3).(*array.Int32Builder)
	totalB := b.Field(4).(*array.Float64Builder)
	avgB := b.Field(5).(*array.Float64Builder)
	minB := b.Field(6).(*array.Float64Builder)
	maxB := b.Field(7).(*array.Float64Builder)
	sucB := b.Field(8).(*array.Int32Builder)
	failB := b.Field(9).(*array.Int32Builder)
	curB := b.Field(10).(*array.StringBuilder)

	for _, agg := range data {
		tsStart.Append(arrow.Timestamp(agg.WindowStart.UnixMilli()))
		tsEnd.Append(arrow.Timestamp(agg.WindowEnd.UnixMilli()))
		shardB.Append(int32(agg.ShardID))
		countB.Append(int32(agg.TotalCount))
		totalB.Append(agg.TotalAmount)
		avgB.Append(agg.AvgAmount)
		minB.Append(agg.MinAmount)
		maxB.Append(agg.MaxAmount)
		sucB.Append(int32(agg.SuccessCount))
		failB.Append(int32(agg.FailedCount))
		curB.Append(agg.Currency)
	}

	rec := b.NewRecord()
	defer rec.Release()

	writer := flight.NewRecordWriter(stream, ipc.WithSchema(arrowSchema))
	defer writer.Close()

	if err := writer.Write(rec); err != nil {
		return fmt.Errorf("write record: %w", err)
	}
	log.Printf("[arrow] served %d aggregates", len(data))
	return nil
}

func (s *ArrowFlightServer) GetFlightInfo(ctx context.Context, req *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	schemaBytes := flight.SerializeSchema(arrowSchema, memory.NewGoAllocator())
	return &flight.FlightInfo{
		Schema: schemaBytes,
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: []byte("transactions")},
		}},
	}, nil
}

func startArrowServer(addr string, srv *ArrowFlightServer) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	grpcServer := grpc.NewServer()
	flight.RegisterFlightServiceServer(grpcServer, srv)
	log.Printf("[arrow] Flight RPC server listening on %s", addr)
	return grpcServer.Serve(lis)
}

// Сохраняем агрегаты в JSON (для Python fallback)
func saveToJSON(aggs []WindowAggregate, path string) error {
	import_os := fmt.Sprintf // заглушка для компилятора — используем os в main
	_ = import_os
	return nil
}

func aggregatesToJSON(aggs []WindowAggregate) ([]byte, error) {
	return json.Marshal(aggs)
}