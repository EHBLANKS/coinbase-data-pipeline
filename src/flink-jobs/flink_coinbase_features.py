import json
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterable, Optional

from pyflink.common import Configuration, WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaOffsetsInitializer,
    DeliveryGuarantee,
)
from pyflink.datastream.connectors.kafka import KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema


# ------------------------------
# Helpers
# ------------------------------

def to_decimal(x: Any) -> Optional[Decimal]:
    if x is None:
        return None
    try:
        return Decimal(str(x))
    except (InvalidOperation, ValueError):
        return None


def extract_tickers(raw_msg: str) -> Iterable[Dict[str, Any]]:
    """
    Parses Coinbase ticker payload and emits normalized rows.
    """

    try:
        data = json.loads(raw_msg)
    except json.JSONDecodeError:
        return []

    exchange_ts = data.get("timestamp")
    sequence_num = data.get("sequence_num")
    events = data.get("events") or []

    results = []

    for ev in events:
        tickers = ev.get("tickers") or []

        for t in tickers:
            symbol = t.get("product_id")

            price = to_decimal(t.get("price"))
            bid = to_decimal(t.get("best_bid"))
            ask = to_decimal(t.get("best_ask"))
            bid_qty = to_decimal(t.get("best_bid_quantity"))
            ask_qty = to_decimal(t.get("best_ask_quantity"))

            if not symbol or bid is None or ask is None:
                continue

            mid = (bid + ask) / Decimal(2)
            spread = ask - bid

            spread_bps = None
            if mid != 0:
                spread_bps = (spread / mid) * Decimal(10_000)

            imbalance = None
            if bid_qty is not None and ask_qty is not None:
                denom = bid_qty + ask_qty
                if denom != 0:
                    imbalance = (bid_qty - ask_qty) / denom

            results.append(
                {
                    "symbol": symbol,
                    "exchange_ts": exchange_ts,
                    "sequence_num": sequence_num,
                    "price": float(price) if price else None,
                    "bid": float(bid),
                    "ask": float(ask),
                    "bid_qty": float(bid_qty) if bid_qty else None,
                    "ask_qty": float(ask_qty) if ask_qty else None,
                    "mid": float(mid),
                    "spread": float(spread),
                    "spread_bps": float(spread_bps) if spread_bps else None,
                    "imbalance": float(imbalance) if imbalance else None,
                }
            )

    return results


def to_json(d: Dict[str, Any]) -> str:
    return json.dumps(d, separators=(",", ":"), ensure_ascii=False)


# ------------------------------
# Main Flink Job
# ------------------------------

def main():
    config = Configuration()
    env = StreamExecutionEnvironment.get_execution_environment(config)

    # Parallelism
    env.set_parallelism(2)

    # Enable checkpointing (every 10 seconds)
    env.enable_checkpointing(10_000)

    # ------------------------------
    # Kafka Source
    # ------------------------------

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("kafka:9092")
        .set_topics("coinbase.raw")
        .set_group_id("flink-coinbase-features")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # ------------------------------
    # Kafka Sink
    # ------------------------------

    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers("kafka:9092")
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("coinbase.features")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    # ------------------------------
    # Processing
    # ------------------------------

    stream = env.from_source(
        source,
        WatermarkStrategy.for_monotonous_timestamps(),
        "coinbase-raw-source"
    )

    features = stream.flat_map(
        lambda raw: [to_json(x) for x in extract_tickers(raw)],
        output_type=Types.STRING()
    )

    features.sink_to(sink)

    # ------------------------------
    # Execute
    # ------------------------------

    env.execute("Coinbase Ticker Feature Engine")


if __name__ == "__main__":
    main()
