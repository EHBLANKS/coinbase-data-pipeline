from dotenv import load_dotenv
from os import getenv
from coinbase.websocket import WSClient
from pipeline.extract.extract import extract_data
load_dotenv()


def main():

    api_key = getenv("CDP_API_KEY_ID","")
    api_secret = getenv("CDP_API_KEY_SECRET","")

    def on_message(raw_msg):
        extract_data(raw_msg)
        pass

    ws_client = WSClient(
        api_key=api_key,
        api_secret=api_secret,
        on_message=on_message,
        verbose=True,
        )

    # open client
    ws_client.open()

    # listen/subscribe to a ticker
    ws_client.subscribe([
        "BTC-USD",
        "ADA-USD",
        "ETH-USD",
        "XRP-USD",
        "SOL-USD",
    ], ["ticker"])
    ws_client.run_forever_with_exception_check()


if __name__ == "__main__":
    main()
