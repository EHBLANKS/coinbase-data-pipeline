from confluent_kafka import Consumer
import asyncio
from json import loads
from src.pipeline.utils.nats import publish_live_tick
from src.pipeline.utils.data import enrich_data
from nats.consumer import connect_nats

consumer = Consumer({
    "bootstrap.servers":"localhost:9092",
    "group.id":"ticker-consumer",
    "auto.offset.reset": "earliest",
})
consumer.subscribe(["coinbase.ticker.raw"])

async def main():
    nc = await connect_nats()
    while True:
        msg = consumer.poll(timeout=1)
        if msg:
            data = enrich_data(msg.value())
            if data:
                await publish_live_tick(nc, data)
            print(f"message: {loads(msg.value().decode('utf-8'))}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        consumer.close()
