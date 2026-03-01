from confluent_kafka import Consumer
import asyncio
from nats.aio.client import Client as NATS

consumer = Consumer(
    {
        "bootstrap.servers": "localhost:9092",
        "group.id": "processor-group",
        "auto.offset.reset": "earliest",
    }
)

async def connect_nats():
    nc = NATS()
    await nc.connect("nats://localhost:4222")
    print("connected to NATS")
    return nc

async def run_consumer():
    print("starting consumer")
    nc = await connect_nats()
    await nc.subscribe("ticks.live")
    try:
        while True:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        pass
    finally:
        await nc.close()



if __name__ == "__main__":
    try:
        asyncio.run(run_consumer())
    except KeyboardInterrupt:
        print("consumer stopped by user")

