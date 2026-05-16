# Лабораторная работа №14

**ФИО:** Глазков Александр Валерьевич  
**Группа:** 221331  
**Вариант:** 24 — Мониторинг финансовых транзакций

## Выполненные задания (повышенная сложность)

| # | Задание | Файлы |
|---|---|---|
| 1 | Распределённый сборщик + etcd | `collector/main.go` |
| 2 | Оконная агрегация (tumbling window) | `collector/window.go` |
| 3 | Apache Arrow Flight RPC | `collector/arrow_server.go`, `analyzer/arrow_client.py` |
| 4 | Rust PyO3 валидатор | `validator/src/lib.rs` |
| 5 | Kubernetes + HPA | `k8s/` |
| 6 | WebSocket дашборд | `dashboard/` |

## Запуск через Docker

```bash
docker compose up --build

# Сервисы:
# Dashboard:   http://localhost:8000
# ETCD:        http://localhost:2379
# Arrow RPC:   localhost:8815
```

## Запуск анализатора

```bash
cd analyzer
pip install -r requirements.txt

# Собери Rust библиотеку (опционально)
cd ../validator && pip install maturin && maturin develop && cd ../analyzer

# Запусти анализ
python main.py
```

## Запуск тестов

```bash
# Go тесты
cd collector && go test ./... -v

# Python тесты
cd analyzer && pytest test_analysis.py -v
```

## Kubernetes (minikube)

```bash
minikube start
eval $(minikube docker-env)
docker build -t lab14-collector:latest ./collector

kubectl apply -f k8s/
kubectl get pods
kubectl get hpa
```

## Производительность

| Этап | Метод | Время |
|---|---|---|
| Загрузка данных | Arrow Flight | ~0.05s |
| Загрузка данных | JSON файлы | ~0.3s |
| Анализ 10K строк | Polars | ~0.02s |
| Анализ 10K строк | DuckDB | ~0.01s |
| Визуализация | Plotly | ~0.1s |