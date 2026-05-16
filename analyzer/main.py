import logging
import sys
from pathlib import Path

import polars as pl

from analysis import (
    aggregate_analysis,
    analyze_with_duckdb,
    clean_and_validate,
    load_from_jsonl,
    save_to_parquet,
)
from arrow_client import fetch_from_arrow_server
from visualize import (
    plot_amount_distribution,
    plot_shard_comparison,
    plot_success_rate_timeline,
    plot_volume_by_currency,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [analyzer] %(levelname)s: %(message)s",
)
log = logging.getLogger(__name__)

# Пытаемся подключить Rust валидатор
try:
    import tx_validator
    RUST_VALIDATOR_AVAILABLE = True
    log.info("Rust validator loaded")
except ImportError:
    RUST_VALIDATOR_AVAILABLE = False
    log.warning("Rust validator not available, skipping validation")

DATA_DIR = "./data"
PARQUET_PATH = f"{DATA_DIR}/transactions.parquet"
ARROW_HOST = "localhost"
ARROW_PORT = 8815


def run_rust_validation(df: pl.DataFrame) -> None:
    """Запускает Rust валидатор на записях (задание 4)."""
    if not RUST_VALIDATOR_AVAILABLE:
        return

    # Конвертируем в список словарей для PyO3
    records = df.to_dicts()
    stats = tx_validator.batch_stats(records)
    log.info(
        "Rust validation: total=%d valid=%d invalid=%d valid_amount=%.2f",
        stats["total"], stats["valid"], stats["invalid"], stats["valid_total_amount"]
    )


def main() -> None:
    Path(DATA_DIR).mkdir(exist_ok=True)

    log.info("=== Step 1: Load data ===")
    # Пробуем Arrow Flight сначала (задание 3)
    df = fetch_from_arrow_server(ARROW_HOST, ARROW_PORT)
    if df is None:
        df = load_from_jsonl(DATA_DIR)

    log.info("Loaded DataFrame: %d rows, columns: %s", len(df), df.columns)
    print("\nFirst 5 rows:")
    print(df.head(5))
    print("\nSchema:")
    print(df.schema)

    log.info("=== Step 2: Clean and validate ===")
    df = clean_and_validate(df)

    # Rust валидация (задание 4)
    run_rust_validation(df)

    log.info("=== Step 3: Aggregate analysis ===")
    agg = aggregate_analysis(df)
    print("\nAggregation by currency:")
    print(agg)

    log.info("=== Step 4: Save to Parquet ===")
    save_to_parquet(df, PARQUET_PATH)

    log.info("=== Step 5: DuckDB analysis ===")
    duck_results = analyze_with_duckdb(PARQUET_PATH)
    print("\nDuckDB - volume by currency:")
    print(duck_results["by_currency"])
    print(f"\nDuckDB query times: "
          f"q1={duck_results['query1_time']:.3f}s "
          f"q2={duck_results['query2_time']:.3f}s "
          f"q3={duck_results['query3_time']:.3f}s")

    log.info("=== Step 6: Visualizations ===")
    plot_volume_by_currency(df, DATA_DIR)
    plot_success_rate_timeline(df, DATA_DIR)
    plot_amount_distribution(df, DATA_DIR)
    plot_shard_comparison(df, DATA_DIR)

    log.info("=== Pipeline complete ===")


if __name__ == "__main__":
    main()
