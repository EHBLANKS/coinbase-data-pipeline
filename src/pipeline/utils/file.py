from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
from pipeline.utils.directory import create_filepath
from config import config

buffer = []


def write_parquet(payload: str) -> None:
    if payload == "":
        return

    print(buffer)
    print(len(buffer))
    now = datetime.now()
    buffer.append(
        {
            "ingest_time": now,
            "exchange": "coinbase",
            "payload": payload,
        }
    )

    if len(buffer) >= config.BUFFER_THRESHOLD:

        try:
            table = pa.Table.from_pylist(buffer)
            create_filepath(f"data/raw/coinbase/{now:%Y/%m/%H}")

            pq.write_to_dataset(
                table=table,
                root_path=f"data/raw/coinbase/{now:%Y/%m/%H}",
            )

        except Exception as e:
            print(f"exception: {e}")

        buffer.clear()
