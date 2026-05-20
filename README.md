# crypto-opportunity-platform

Real-Time Crypto Opportunity Detection Platform focused on:

- event-driven architecture,
- realtime stream processing,
- distributed systems,
- trading signal analytics,
- scalable data engineering pipelines.

## Tech Stack

- Python
- Apache Kafka
- Spark Structured Streaming
- FastAPI
- ClickHouse
- Docker
- Prometheus
- Grafana

### Current pipeline

```text
Binance WebSocket
        ↓
Dockerized ingestion-service
        ↓
Apache Kafka (raw.trades topic)
```

### Example tracked symbols

- BTCUSDT
- ETHUSDT
- SOLUSDT
- BNBUSDT
- XRPUSDT
- DOGEUSDT
