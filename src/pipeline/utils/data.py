from models.coinbase import EnrichedTicker
from json import loads, JSONDecodeError
from logging import Logger
logger = Logger("coinbase")

def enrich_data(message: str | bytes) -> EnrichedTicker | None:
    try:
        msg = loads(message)
        data = EnrichedTicker.model_validate(msg)
        return data

    except JSONDecodeError as e:
        logger.error(f"json parsing error: {e}")

    except Exception as e:
        logger.warning(f"couldn't validate: {e}")




