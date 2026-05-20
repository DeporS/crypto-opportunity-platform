import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import Literal

import websockets
from aiokafka import AIOKafkaProducer
from pydantic import BaseModel
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    kafka_bootstrap_servers: str = "kafka:29092"
    kafka_topic: str = "raw.trades"
    binance_ws_url: str = "wss://stream.binance.com:9443/ws/btcusdt@trade"


settings = Settings()

class TradeEvent(BaseModel):
    event_id: str
    exchange: str
    symbol: str
    price: float
    quantity: float
    side: Literal["buy", "sell"]
    trade_timestamp: datetime
    ingestion_timestamp: datetime


def map_binance_trade(raw_message: dict) -> TradeEvent:
    side = "sell" if raw_message["m"] else "buy"

    return TradeEvent(
        event_id=str(uuid.uuid4()),
        exchange="binance",
        symbol=raw_message["s"],
        price=float(raw_message["p"]),
        quantity=float(raw_message["q"]),
        side=side,
        trade_timestamp=datetime.fromtimestamp(raw_message["T"] / 1000, tz=timezone.utc),
        ingestion_timestamp=datetime.now(timezone.utc),
    )



async def run_producer() -> None:
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    await producer.start()
    print("Kafka producer started")

    try:
        async with websockets.connect(settings.binance_ws_url) as websocket:
            print("Connected to Binance WebSocket")

            async for message in websocket:
                raw_message = json.loads(message)
                trade_event = map_binance_trade(raw_message)

                await producer.send_and_wait(
                    settings.kafka_topic,
                    value=trade_event.model_dump(mode="json"),
                    key=trade_event.symbol.encode("utf-8"),
                )

                print(
                    f"Sent trade: {trade_event.symbol} "
                    f"{trade_event.side} "
                    f"{trade_event.price} "
                    f"{trade_event.quantity}"
                )

    finally:
        await producer.stop()
        print("Kafka producer stopped")


async def main() -> None:
    while True:
        try:
            await run_producer()
        except Exception as exc:
            print(f"Ingestion error: {exc}")
            print("Restarting ingestion in 5 seconds...")
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())