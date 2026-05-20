import asyncio
import json
import logging
import signal
import uuid
from datetime import datetime, timezone
from typing import Literal

import websockets
from aiokafka import AIOKafkaProducer
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    kafka_bootstrap_servers: str = "kafka:29092"
    kafka_topic: str = "raw.trades"
    kafka_dlq_topic: str = "dlq.trades"

    exchange: str = "binance"
    symbols: str = "btcusdt,ethusdt,solusdt,bnbusdt,xrpusdt,dogeusdt"
    reconnect_delay_seconds: int = 5

    log_level: str = "INFO"

    @property
    def symbols_list(self) -> list[str]:
        return [symbol.strip().lower() for symbol in self.symbols.split(",") if symbol.strip()]

    @property
    def binance_ws_url(self) -> str:
        streams = "/".join(f"{symbol}@trade" for symbol in self.symbols_list)
        return f"wss://stream.binance.com:9443/stream?streams={streams}"


settings = Settings()

logging.basicConfig(
    level=settings.log_level,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger("ingestion-service")


class TradeEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    exchange: str
    symbol: str
    price: float
    quantity: float
    side: Literal["buy", "sell"]
    trade_timestamp: datetime
    ingestion_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class InvalidTradeEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    exchange: str
    raw_payload: dict
    error_message: str
    ingestion_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


def map_binance_trade(raw_message: dict) -> TradeEvent:
    """
    Maps raw Binance trade payload into internal normalized event schema.
    """

    # Binance:
    # m=True means buyer is market maker,
    # therefore aggressive side is sell.
    side = "sell" if raw_message["m"] else "buy"

    return TradeEvent(
        exchange=settings.exchange,
        symbol=raw_message["s"],
        price=float(raw_message["p"]),
        quantity=float(raw_message["q"]),
        side=side,
        trade_timestamp=datetime.fromtimestamp(raw_message["T"] / 1000, tz=timezone.utc),
    )


def extract_trade_payload(message: str) -> dict:
    payload = json.loads(message)

    # Binance combined streams wrap payload inside "data".
    if "data" in payload:
        return payload["data"]

    return payload


async def send_to_dlq(
    producer: AIOKafkaProducer,
    raw_payload: dict,
    error: Exception,
) -> None:
    dlq_event = InvalidTradeEvent(
        exchange=settings.exchange,
        raw_payload=raw_payload,
        error_message=str(error),
    )

    await producer.send_and_wait(
        settings.kafka_dlq_topic,
        value=dlq_event.model_dump(mode="json"),
        # Use symbol as Kafka key to preserve event ordering
        # for a given trading pair within the same partition.
        key=settings.exchange.encode("utf-8"),
    )


async def run_ingestion(stop_event: asyncio.Event) -> None:
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        enable_idempotence=True,
    )

    await producer.start()

    logger.info(
        "Kafka producer started | bootstrap_servers=%s | topic=%s | symbols=%s",
        settings.kafka_bootstrap_servers,
        settings.kafka_topic,
        ",".join(settings.symbols_list),
    )

    try:
        while not stop_event.is_set():
            try:
                logger.info("Connecting to Binance WebSocket | url=%s", settings.binance_ws_url)

                async with websockets.connect(
                    settings.binance_ws_url,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10,
                ) as websocket:
                    logger.info("Connected to Binance WebSocket")

                    async for message in websocket:
                        if stop_event.is_set():
                            break

                        raw_trade = {}

                        try:
                            raw_trade = extract_trade_payload(message)
                            trade_event = map_binance_trade(raw_trade)

                            await producer.send_and_wait(
                                settings.kafka_topic,
                                value=trade_event.model_dump(mode="json"),
                                key=trade_event.symbol.encode("utf-8"),
                            )

                            logger.debug(
                                "Trade event produced | symbol=%s | side=%s | price=%s | quantity=%s",
                                trade_event.symbol,
                                trade_event.side,
                                trade_event.price,
                                trade_event.quantity,
                            )

                        except Exception as event_error:
                            logger.exception("Failed to process trade event")
                            await send_to_dlq(producer, raw_trade, event_error)

            except Exception:
                logger.exception(
                    "WebSocket connection failed. Reconnecting in %s seconds...",
                    settings.reconnect_delay_seconds,
                )
                await asyncio.sleep(settings.reconnect_delay_seconds)

    finally:
        await producer.stop()
        logger.info("Kafka producer stopped")


async def main() -> None:
    stop_event = asyncio.Event()

    loop = asyncio.get_running_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    logger.info("Starting ingestion service")
    await run_ingestion(stop_event)


if __name__ == "__main__":
    asyncio.run(main())