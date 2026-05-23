import json
import logging
import os
import time
from typing import Any, Dict

import psycopg2
from kafka import KafkaConsumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "funding_rates")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "funding-rates-consumer")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres_data")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "trading")
POSTGRES_USER = os.getenv("POSTGRES_USER", "trading")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "trading")


def get_db_connection() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def ensure_table(conn: psycopg2.extensions.connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS funding_rates (
                id SERIAL PRIMARY KEY,
                base_asset_id BIGINT,
                base_asset TEXT,
                funding_rate DOUBLE PRECISION,
                event_time BIGINT,
                ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
    conn.commit()


def parse_message(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    if isinstance(raw, str):
        raw = json.loads(raw)
    if not isinstance(raw, dict):
        raise ValueError("Message payload is not a JSON object")
    return raw


def insert_record(conn: psycopg2.extensions.connection, payload: Dict[str, Any]) -> None:
    base_asset_id = payload.get("baseAssetId")
    base_asset = payload.get("baseAsset")
    funding_rate = payload.get("fundingRate")
    event_time = payload.get("eventTime")

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO funding_rates (base_asset_id, base_asset, funding_rate, event_time)
            VALUES (%s, %s, %s, %s)
            """,
            (base_asset_id, base_asset, funding_rate, event_time),
        )
    conn.commit()


def run() -> None:
    while True:
        try:
            conn = get_db_connection()
            ensure_table(conn)
            break
        except Exception as exc:
            logging.warning("Waiting for Postgres: %s", exc)
            time.sleep(3)

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=KAFKA_GROUP_ID,
        value_deserializer=lambda v: v,
    )

    logging.info("Consumer started. topic=%s broker=%s", KAFKA_TOPIC, KAFKA_BOOTSTRAP_SERVERS)

    try:
        for msg in consumer:
            try:
                payload = parse_message(msg.value)
                insert_record(conn, payload)
                logging.info("Inserted funding rate for base_asset=%s", payload.get("baseAsset"))
            except Exception as exc:
                logging.exception("Failed to process message: %s", exc)
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    run()
