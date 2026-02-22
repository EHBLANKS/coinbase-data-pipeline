import pyarrow
from models.coinbase import Ticker
from json import JSONDecodeError, loads
from logging import Logger

logger = Logger("Coinbase")


def extract_data(message:str):
    try:
        msg = loads(message)
        data = Ticker.model_validate(msg["events"][0]["tickers"][0])
        return data

    except JSONDecodeError as e:
        logger.error(f"json parsing error: {e}")

    except Exception as e:
        logger.warning(f"couldn't validate: {e}")


