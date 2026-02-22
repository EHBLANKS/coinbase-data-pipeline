from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
from models.coinbase import Ticker
from utils.directory import create_filepath

buffer = []

def write_parquet(payload:Ticker) -> None:
    now = datetime.now()
    buffer.append({
        "ingest_time": now
    })

    if len(buffer) >= 500:
        table = pa.Table.from_mylist(buffer)
        create_filepath(f"data/raw/coinbase/{now:%Y/%m/%H}")

        pq.write_to_dataset(
            table,
            root_path="data/raw/coinbase",
            partition_cols=["exchange"],
        )

