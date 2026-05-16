import asyncio
import json
import logging
import os
import random
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from starlette.requests import Request

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

DATA_DIR = os.getenv("DATA_DIR", "./data")
templates = Jinja2Templates(directory="templates")

# Хранилище данных в памяти
aggregates: list = []
clients: Set[WebSocket] = set()


def load_data_from_files() -> list:
    """Загружает данные из JSONL файлов сборщика."""
    records = []
    for path in Path(DATA_DIR).glob("aggregates_shard*.jsonl"):
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
    return records


def generate_demo_data(n: int = 50) -> list:
    """Демо-данные если сборщик не запущен."""
    currencies = ["RUB", "USD", "EUR", "CNY"]
    records = []
    now = datetime.now()
    for i in range(n):
        t = now - timedelta(minutes=i * 2)
        total = random.uniform(100000, 3000000)
        count = random.randint(50, 300)
        suc = int(count * (0.8 + random.uniform(0, 0.15)))
        records.append({
            "window_start": t.isoformat(),
            "window_end": (t + timedelta(seconds=10)).isoformat(),
            "shard_id": random.randint(1, 2),
            "total_count": count,
            "total_amount": total,
            "avg_amount": total / count,
            "min_amount": random.uniform(10, 500),
            "max_amount": random.uniform(50000, 200000),
            "success_count": suc,
            "failed_count": count - suc,
            "currency": random.choice(currencies),
            "success_rate": suc / count * 100,
            "failure_rate": (count - suc) / count * 100,
        })
    return records


def compute_stats(data: list) -> dict:
    if not data:
        return {"avg_count": 0, "avg_volume": 0, "success_rate": 0, "shards": 0}
    avg_count = sum(d["total_count"] for d in data) / len(data)
    avg_volume = sum(d["total_amount"] for d in data) / len(data)
    success_rates = [d.get("success_rate", 0) for d in data if "success_rate" in d]
    avg_success = sum(success_rates) / len(success_rates) if success_rates else 0
    shards = len(set(d["shard_id"] for d in data))
    return {
        "avg_count": avg_count,
        "avg_volume": avg_volume,
        "success_rate": avg_success,
        "shards": shards,
    }


async def broadcast(message: dict) -> None:
    disconnected = set()
    for ws in clients:
        try:
            await ws.send_json(message)
        except Exception:
            disconnected.add(ws)
    clients -= disconnected


async def data_updater() -> None:
    """Фоновая задача — обновляет данные каждые 2 секунды."""
    while True:
        try:
            # Пробуем загрузить реальные данные
            real_data = load_data_from_files()
            data = real_data if real_data else generate_demo_data(50)

            # Добавляем success_rate если нет
            for d in data:
                if "success_rate" not in d and d.get("total_count", 0) > 0:
                    d["success_rate"] = d.get("success_count", 0) / d["total_count"] * 100
                    d["failure_rate"] = d.get("failed_count", 0) / d["total_count"] * 100

            stats = compute_stats(data)
            await broadcast({"type": "update", "data": data[-100:], "stats": stats})
        except Exception as e:
            log.error("Update error: %s", e)
        await asyncio.sleep(2)


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(data_updater())
    yield
    task.cancel()


app = FastAPI(title="Financial Monitor Dashboard", lifespan=lifespan)


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    clients.add(ws)
    log.info("Client connected, total=%d", len(clients))
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        clients.discard(ws)
        log.info("Client disconnected, total=%d", len(clients))


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "clients": len(clients)}
