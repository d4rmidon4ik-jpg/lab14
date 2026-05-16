import logging
import time
from typing import Optional

import polars as pl
import pyarrow as pa
import pyarrow.flight as flight

log = logging.getLogger(__name__)


def fetch_from_arrow_server(host: str = "localhost", port: int = 8815) -> Optional[pl.DataFrame]:
    """Получает агрегированные данные от Go Arrow Flight сервера."""
    try:
        client = flight.FlightClient(f"grpc://{host}:{port}")
        ticket = flight.Ticket(b"transactions")

        start = time.time()
        reader = client.do_get(ticket)
        table = reader.read_all()
        elapsed = time.time() - start

        df = pl.from_arrow(table)
        log.info(
            "Arrow Flight: fetched %d rows in %.3fs, size=%.1fKB",
            len(df), elapsed, table.nbytes / 1024
        )
        return df
    except Exception as e:
        log.warning("Arrow Flight unavailable: %s, falling back to JSON", e)
        return None