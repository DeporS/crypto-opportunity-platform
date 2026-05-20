# crypto-opportunity-platform

# Project structure

├── services/
│ ├── ingestion-service/
│ ├── stream-processing/
│ ├── signal-engine/
│ ├── alert-service/
│ ├── strategy-engine/
│ ├── api-gateway/
│ └── backtesting-engine/
│
├── shared/
│ ├── schemas/
│ ├── utils/
│ ├── configs/
│ └── clients/
│
├── infra/
│ ├── docker/
│ ├── kafka/
│ ├── spark/
│ ├── grafana/
│ ├── prometheus/
│ └── airflow/
│
├── data/
│ ├── parquet/
│ ├── checkpoints/
│ └── replay/
│
├── notebooks/
│
├── dashboards/
│
├── docs/
│ ├── architecture/
│ ├── diagrams/
│ └── ADRs/
│
└── docker-compose.yml
