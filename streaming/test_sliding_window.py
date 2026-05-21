import time
from datetime import datetime, timedelta

import pytest

from nats_consumer import SlidingWindowProcessor


@pytest.fixture
def processor():
    return SlidingWindowProcessor(window_minutes=5)


def make_record(amount: float = 1000.0, count: int = 10,
                success: int = 9, currency: str = "RUB") -> dict:
    return {
        "total_amount": amount,
        "total_count": count,
        "success_count": success,
        "failed_count": count - success,
        "currency": currency,
        "shard_id": 1,
    }


def test_add_record(processor):
    processor.add(make_record())
    assert len(processor.window) == 1
    assert processor.processed == 1


def test_stats_empty(processor):
    stats = processor.compute_stats()
    assert stats["window_size"] == 0


def test_stats_with_data(processor):
    processor.add(make_record(amount=1000, count=10, success=8))
    processor.add(make_record(amount=2000, count=20, success=18))
    stats = processor.compute_stats()
    assert stats["window_size"] == 2
    assert stats["total_transactions"] == 30
    assert stats["total_volume"] == 3000.0
    assert stats["success_rate"] == pytest.approx(86.67, rel=0.01)


def test_eviction_of_old_records(processor):
    # Добавляем старую запись вручную
    old_time = datetime.now() - timedelta(minutes=6)
    processor.window.append({"received_at": old_time, "data": make_record()})
    processor.add(make_record())  # новая запись

    stats = processor.compute_stats()
    # Старая должна быть вычищена
    assert stats["window_size"] == 1


def test_to_dataframe_empty(processor):
    df = processor.to_dataframe()
    assert len(df) == 0


def test_to_dataframe_with_data(processor):
    processor.add(make_record(currency="RUB"))
    processor.add(make_record(currency="USD"))
    df = processor.to_dataframe()
    assert len(df) == 2
    assert "currency" in df.columns


def test_multiple_evictions(processor):
    # Добавляем 3 старых и 2 новых
    old_time = datetime.now() - timedelta(minutes=10)
    for _ in range(3):
        processor.window.append({"received_at": old_time, "data": make_record()})
    processor.add(make_record())
    processor.add(make_record())

    stats = processor.compute_stats()
    assert stats["window_size"] == 2
