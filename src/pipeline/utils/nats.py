from json import dumps
from nats.aio.client import Client as NATS
from models.coinbase import EnrichedTicker

async def publish_live_tick(nats: NATS, ticker: EnrichedTicker):
    msg = {
        "symbol": ticker.product_id,
        "price": str(ticker.price),
        "best_bid": str(ticker.best_bid),
        "best_ask": str(ticker.best_ask),
        "best_bid_quantity": str(ticker.best_bid_quantity),
        "best_ask_quantity": str(ticker.best_ask_quantity),
        "spread": ticker.spread,
        "mid": ticker.mid,
        "imbalance": ticker.imbalance
    }

    await nats.publish("ticks.live", dumps(msg).encode())

