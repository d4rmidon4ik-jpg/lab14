"""
Задание 7: Python-консьюмер из NATS с обработкой скользящим окном (5 минут).
Go-продюсер пишет агрегаты в топик transactions.aggregates,
Python читает и накапливает скользящее окно.
"""
import asyncio
import json
import logging
import os
import time
from collections import deque
from datetime import datetime, timedelta

import nats
import polars as pl

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [nats-consumer] %(levelname)s: %(message)s",
)
log = logging.getLogger(__name__)

NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
TOPIC = "transactions.aggregates"
SLIDING_WINDOW_MINUTES = 5


class SlidingWindowProcessor:
    """
    Скользящее окно в 5 минут.
    Хранит все агрегаты за последние 5 минут,
    каждые 30 секунд вычисляет статистику по окну.
    """

    def __init__(self, window_minutes: int = 5):
        self.window = deque()
        self.window_duration = timedelta(minutes=window_minutes)
        self.processed = 0

    def add(self, record: dict) -> None:
        now = datetime.now()
        self.window.append({"received_at": now, "data": record})
        self._evict_old(now)
        self.processed += 1

    def _evict_old(self, now: datetime) -> None:
        """Удаляет записи старше window_duration."""
        cutoff = now - self.window_duration
        while self.window and self.window[0]["received_at"] < cutoff:
            self.window.popleft()

    def compute_stats(self) -> dict:
        """Вычисляет статистику по текущему скользящему окну."""
        self._evict_old(datetime.now())
        if not self.window:
            return {"window_size": 0, "window_minutes": SLIDING_WINDOW_MINUTES}

        records = [item["data"] for item in self.window]
        total_tx = sum(r.get("total_count", 0) for r in records)
        total_vol = sum(r.get("total_amount", 0.0) for r in records)
        success = sum(r.get("success_count", 0) for r in records)
        failed = sum(r.get("failed_count", 0) for r in records)

        return {
            "window_size": len(records),
            "window_minutes": SLIDING_WINDOW_MINUTES,
            "total_transactions": total_tx,
            "total_volume": total_vol,
            "avg_volume_per_window": total_vol / len(records),
            "success_count": success,
            "failed_count": failed,
            "success_rate": success / max(total_tx, 1) * 100,
            "computed_at": datetime.now().isoformat(),
        }

    def to_dataframe(self) -> pl.DataFrame:
        """Возвращает текущее окно как Polars DataFrame."""
        self._evict_old(datetime.now())
        if not self.window:
            return pl.DataFrame()
        records = [item["data"] for item in self.window]
        return pl.DataFrame(records)


async def stats_reporter(processor: SlidingWindowProcessor) -> None:
    """Каждые 30 секунд печатает статистику скользящего окна."""
    while True:
        await asyncio.sleep(30)
        stats = processor.compute_stats()
        log.info(
            "Sliding window [%d min]: %d records, %d tx, volume=%.0f, success=%.1f%%",
            stats["window_minutes"],
            stats["window_size"],
            stats["total_transactions"],
            stats["total_volume"],
            stats["success_rate"],
        )

        df = processor.to_dataframe()
        if len(df) > 0:
            agg = df.group_by("currency").agg(
                pl.col("total_amount").sum().alias("volume"),
                pl.col("total_count").sum().alias("count"),
            ).sort("volume", descending=True)
            log.info("Volume by currency (window):\n%s", agg)


async def main() -> None:
    processor = SlidingWindowProcessor(SLIDING_WINDOW_MINUTES)

    nc = await nats.connect(NATS_URL)
    log.info("Connected to NATS at %s", NATS_URL)

    async def message_handler(msg) -> None:
        try:
            record = json.loads(msg.data.decode())
            processor.add(record)
            log.info(
                "Received aggregate: shard=%s count=%d amount=%.0f",
                record.get("shard_id"),
                record.get("total_count", 0),
                record.get("total_amount", 0),
            )
        except Exception as e:
            log.error("Message handling error: %s", e)

    await nc.subscribe(TOPIC, cb=message_handler)
    log.info("Subscribed to %s, sliding window=%d min", TOPIC, SLIDING_WINDOW_MINUTES)

    reporter_task = asyncio.create_task(stats_reporter(processor))

    try:
        await asyncio.Future()
    except asyncio.CancelledError:
        pass
    finally:
        reporter_task.cancel()
        await nc.close()
        log.info("Consumer stopped, total processed: %d", processor.processed)


if __name__ == "__main__":
    asyncio.run(main())
