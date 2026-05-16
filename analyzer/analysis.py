import json
import logging
import time
from pathlib import Path
from typing import Optional

import duckdb
import polars as pl

log = logging.getLogger(__name__)


def load_from_jsonl(data_dir: str = "./data") -> pl.DataFrame:
    """Загружает агрегаты из JSONL файлов (fallback от Arrow)."""
    records = []
    for path in Path(data_dir).glob("aggregates_shard*.jsonl"):
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))

    if not records:
        log.warning("No JSONL files found, generating sample data")
        return _generate_sample_data()

    df = pl.DataFrame(records)
    log.info("Loaded %d records from JSONL", len(df))
    return df


def _generate_sample_data() -> pl.DataFrame:
    """Генерирует тестовые данные если сборщик ещё не запущен."""
    import random
    from datetime import datetime, timedelta

    records = []
    now = datetime.now()
    currencies = ["RUB", "USD", "EUR", "CNY"]

    for i in range(200):
        window_start = now - timedelta(minutes=i * 10)
        total = random.uniform(100000, 5000000)
        count = random.randint(50, 500)
        records.append({
            "window_start": window_start.isoformat(),
            "window_end": (window_start + timedelta(seconds=10)).isoformat(),
            "shard_id": random.randint(1, 2),
            "total_count": count,
            "total_amount": total,
            "avg_amount": total / count,
            "min_amount": random.uniform(10, 1000),
            "max_amount": random.uniform(50000, 100000),
            "success_count": int(count * 0.85),
            "failed_count": int(count * 0.10),
            "currency": random.choice(currencies),
        })

    return pl.DataFrame(records)


def clean_and_validate(df: pl.DataFrame) -> pl.DataFrame:
    """Очистка и валидация данных (задание 5 средних)."""
    initial_count = len(df)

    # Удаляем дубликаты
    df = df.unique()

    # Заполняем пропуски в currency (до фильтрации, чтобы не потерять UNKNOWN)
    df = df.with_columns(
        pl.col("currency").fill_null("UNKNOWN")
    )

    # Удаляем строки с некорректными суммами
    df = df.filter(pl.col("total_amount") > 0)
    df = df.filter(pl.col("total_count") > 0)
    df = df.filter(pl.col("avg_amount") > 0)

    # Добавляем вычисляемые поля
    df = df.with_columns([
        (pl.col("success_count") / pl.col("total_count") * 100)
        .alias("success_rate"),
        (pl.col("failed_count") / pl.col("total_count") * 100)
        .alias("failure_rate"),
    ])

    log.info("Cleaned: %d → %d rows (removed %d)", initial_count, len(df), initial_count - len(df))
    return df


def aggregate_analysis(df: pl.DataFrame) -> pl.DataFrame:
    """Агрегационный анализ (задание 6 средних)."""
    return df.group_by("currency").agg([
        pl.col("total_amount").sum().alias("total_volume"),
        pl.col("total_amount").mean().alias("avg_window_volume"),
        pl.col("total_amount").min().alias("min_window_volume"),
        pl.col("total_amount").max().alias("max_window_volume"),
        pl.col("total_count").sum().alias("total_transactions"),
        pl.col("success_rate").mean().alias("avg_success_rate"),
        pl.col("failure_rate").mean().alias("avg_failure_rate"),
    ]).sort("total_volume", descending=True)


def save_to_parquet(df: pl.DataFrame, path: str = "./data/transactions.parquet") -> None:
    """Сохраняет в Parquet (задание 7 средних)."""
    df.write_parquet(path)
    size_mb = Path(path).stat().st_size / 1024 / 1024
    log.info("Saved to Parquet: %s (%.2f MB, %d rows)", path, size_mb, len(df))


def analyze_with_duckdb(parquet_path: str = "./data/transactions.parquet") -> dict:
    """Анализ через DuckDB с замером времени (задание 8 средних)."""
    conn = duckdb.connect()
    results = {}

    # Запрос 1: объём по валютам
    start = time.perf_counter()
    results["by_currency"] = conn.execute(f"""
        SELECT
            currency,
            SUM(total_amount) as total_volume,
            AVG(avg_amount) as avg_transaction,
            SUM(total_count) as transaction_count,
            AVG(success_rate) as avg_success_rate
        FROM '{parquet_path}'
        WHERE total_amount > 0
        GROUP BY currency
        ORDER BY total_volume DESC
    """).df()
    results["query1_time"] = time.perf_counter() - start

    # Запрос 2: топ окна по объёму
    start = time.perf_counter()
    results["top_windows"] = conn.execute(f"""
        SELECT
            window_start,
            shard_id,
            total_count,
            total_amount,
            currency,
            success_rate
        FROM '{parquet_path}'
        ORDER BY total_amount DESC
        LIMIT 10
    """).df()
    results["query2_time"] = time.perf_counter() - start

    # Запрос 3: статистика по шардам
    start = time.perf_counter()
    results["by_shard"] = conn.execute(f"""
        SELECT
            shard_id,
            COUNT(*) as windows,
            SUM(total_count) as total_transactions,
            SUM(total_amount) as total_volume,
            AVG(failure_rate) as avg_failure_rate
        FROM '{parquet_path}'
        GROUP BY shard_id
        ORDER BY shard_id
    """).df()
    results["query3_time"] = time.perf_counter() - start

    log.info(
        "DuckDB queries: q1=%.3fs q2=%.3fs q3=%.3fs",
        results["query1_time"], results["query2_time"], results["query3_time"]
    )
    return results
