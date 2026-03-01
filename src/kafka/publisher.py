from confluent_kafka import Producer

producer = Producer(
    {
        "bootstrap.servers": "localhost:9094",
        "enable.idempotence": True,
        "acks":"all",
        "compression.type": "lz4",
    }
)

def publish_msg(symbol, raw_msg:str):
    producer.produce(
        topic="coinbase.raw",
        key=symbol,
        value=raw_msg,
    )
