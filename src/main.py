from dotenv import load_dotenv
from os import getenv
from coinbase.websocket import WSClient
from pipeline.extract.extract import extract_data
from pipeline.utils.file import write_parquet
from kafka.publisher import publish_msg

load_dotenv()


def main():
    api_key = getenv("CDP_API_KEY_ID", "")
    api_secret = getenv("CDP_API_KEY_SECRET", "")

    def on_message(raw_msg):
        ticker = extract_data(raw_msg)

        # save in in disk
        write_parquet(raw_msg)

        # send message to kafka and nats
        if ticker:
            publish_msg(ticker.product_id,raw_msg)

    ws_client = WSClient(
        api_key=api_key,
        api_secret=api_secret,
        on_message=on_message,
        verbose=True,
    )

    ws_client.open()

    ws_client.subscribe(
        ["BTC-USD", "ADA-USD", "ETH-USD", "XRP-USD", "SOL-USD"],
        ["ticker"],
    )

    ws_client.run_forever_with_exception_check()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("closing...")
