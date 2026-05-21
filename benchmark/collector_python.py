"""
Задание 6: Python-сборщик транзакций на asyncio/aiohttp.
Аналог Go-сборщика для сравнения производительности.
"""
import asyncio
import json
import os
import random
import time
from pathlib import Path

SHARD_ID = int(os.getenv("SHARD_ID", "3"))
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "./data")
WINDOW_SIZE = int(os.getenv("WINDOW_SIZE", "10"))

CURRENCIES = ["RUB", "USD", "EUR", "CNY"]
TX_TYPES = ["transfer", "payment", "withdrawal"]
STATUSES = ["success", "success", "success", "failed", "pending"]


def generate_transaction(shard_id: int) -> dict:
    return {
        "id": f"TXN-PY-{shard_id}-{time.time_ns()}",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "amount": random.uniform(10, 100000),
        "currency": random.choice(CURRENCIES),
        "account_from": f"ACC{random.randint(0, 99999999):08d}",
        "account_to": f"ACC{random.randint(0, 99999999):08d}",
        "type": random.choice(TX_TYPES),
        "status": random.choice(STATUSES),
        "shard_id": shard_id,
    }


def aggregate_window(transactions: list, window_end: float) -> dict:
    if not transactions:
        return {}
    amounts = [t["amount"] for t in transactions]
    success = sum(1 for t in transactions if t["status"] == "success")
    failed = sum(1 for t in transactions if t["status"] == "failed")
    currencies = {}
    for t in transactions:
        currencies[t["currency"]] = currencies.get(t["currency"], 0) + 1
    dominant = max(currencies, key=currencies.get)
    return {
        "window_start": transactions[0]["timestamp"],
        "window_end": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "shard_id": SHARD_ID,
        "total_count": len(transactions),
        "total_amount": sum(amounts),
        "avg_amount": sum(amounts) / len(amounts),
        "min_amount": min(amounts),
        "max_amount": max(amounts),
        "success_count": success,
        "failed_count": failed,
        "currency": dominant,
    }


async def run_collector(duration_sec: int = 30) -> dict:
    """Запускает сборщик на duration_sec секунд, возвращает метрики."""
    Path(OUTPUT_DIR).mkdir(exist_ok=True)
    output_file = f"{OUTPUT_DIR}/aggregates_shard{SHARD_ID}.jsonl"

    transactions = []
    windows_flushed = 0
    total_tx = 0
    start_time = time.perf_counter()
    window_start = start_time

    import tracemalloc
    tracemalloc.start()

    while time.perf_counter() - start_time < duration_sec:
        # Генерируем пакет транзакций (имитация 10 tx/sec)
        batch = [generate_transaction(SHARD_ID) for _ in range(10)]
        transactions.extend(batch)
        total_tx += len(batch)

        # Tumbling window flush
        if time.perf_counter() - window_start >= WINDOW_SIZE:
            agg = aggregate_window(transactions, time.perf_counter())
            if agg:
                with open(output_file, "a") as f:
                    f.write(json.dumps(agg) + "\n")
                windows_flushed += 1
            transactions = []
            window_start = time.perf_counter()

        await asyncio.sleep(0.1)  # 100ms интервал

    # Финальный flush
    if transactions:
        agg = aggregate_window(transactions, time.perf_counter())
        if agg:
            with open(output_file, "a") as f:
                f.write(json.dumps(agg) + "\n")
            windows_flushed += 1

    elapsed = time.perf_counter() - start_time
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    return {
        "language": "Python (asyncio)",
        "duration_sec": elapsed,
        "total_transactions": total_tx,
        "tx_per_sec": total_tx / elapsed,
        "windows_flushed": windows_flushed,
        "peak_memory_mb": peak / 1024 / 1024,
    }


if __name__ == "__main__":
    print("Starting Python collector benchmark (30 seconds)...")
    result = asyncio.run(run_collector(30))
    print(json.dumps(result, indent=2))
