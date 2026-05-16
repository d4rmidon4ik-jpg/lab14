import logging
from pathlib import Path

import plotly.express as px
import plotly.graph_objects as go
import polars as pl

log = logging.getLogger(__name__)


def plot_volume_by_currency(df: pl.DataFrame, out_dir: str = "./data") -> str:
    """График объёма транзакций по валютам."""
    agg = df.group_by("currency").agg(
        pl.col("total_amount").sum().alias("volume")
    ).sort("volume", descending=True)

    fig = px.bar(
        agg.to_pandas(),
        x="currency", y="volume",
        title="Объём транзакций по валютам",
        labels={"currency": "Валюта", "volume": "Объём (руб.)"},
        color="currency",
    )
    path = f"{out_dir}/volume_by_currency.html"
    fig.write_html(path)
    log.info("Saved: %s", path)
    return path


def plot_success_rate_timeline(df: pl.DataFrame, out_dir: str = "./data") -> str:
    """Временной ряд успешности транзакций."""
    pandas_df = df.sort("window_start").to_pandas()

    fig = go.Figure()
    for shard in pandas_df["shard_id"].unique():
        shard_df = pandas_df[pandas_df["shard_id"] == shard]
        fig.add_trace(go.Scatter(
            x=shard_df["window_start"],
            y=shard_df["success_rate"],
            name=f"Shard {shard}",
            mode="lines+markers",
        ))

    fig.update_layout(
        title="Успешность транзакций во времени",
        xaxis_title="Время",
        yaxis_title="% успешных",
        yaxis=dict(range=[0, 105]),
    )
    path = f"{out_dir}/success_rate_timeline.html"
    fig.write_html(path)
    log.info("Saved: %s", path)
    return path


def plot_amount_distribution(df: pl.DataFrame, out_dir: str = "./data") -> str:
    """Гистограмма распределения средних сумм."""
    fig = px.histogram(
        df.to_pandas(),
        x="avg_amount",
        nbins=30,
        title="Распределение средних сумм транзакций",
        labels={"avg_amount": "Средняя сумма"},
        color_discrete_sequence=["#636EFA"],
    )
    path = f"{out_dir}/amount_distribution.html"
    fig.write_html(path)
    log.info("Saved: %s", path)
    return path


def plot_shard_comparison(df: pl.DataFrame, out_dir: str = "./data") -> str:
    """Сравнение производительности шардов."""
    shard_agg = df.group_by("shard_id").agg([
        pl.col("total_count").sum().alias("transactions"),
        pl.col("total_amount").sum().alias("volume"),
        pl.col("failure_rate").mean().alias("failure_rate"),
    ]).sort("shard_id")

    fig = px.bar(
        shard_agg.to_pandas(),
        x="shard_id", y="transactions",
        title="Транзакции по шардам",
        labels={"shard_id": "Shard ID", "transactions": "Количество"},
        color="failure_rate",
        color_continuous_scale="RdYlGn_r",
    )
    path = f"{out_dir}/shard_comparison.html"
    fig.write_html(path)
    log.info("Saved: %s", path)
    return path
