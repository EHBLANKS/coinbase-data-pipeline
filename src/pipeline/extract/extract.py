from models.coinbase import Ticker
from json import JSONDecodeError, loads
from logging import Logger

logger = Logger("Coinbase")


def extract_data(message: str | bytes) -> Ticker | None:
    try:
        msg = loads(message)
        tickers = msg.get("events", [{}])[0].get("tickers")
        if not tickers:
            return None
        data = Ticker.model_validate(tickers[0])
        return data

    except JSONDecodeError as e:
        logger.error(f"json parsing error: {e}")

    except Exception as e:
        logger.warning(f"couldn't validate: {e}")




