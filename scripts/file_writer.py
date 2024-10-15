import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

base_dir = os.path.dirname(os.path.abspath(__file__))


class FileWriter:
    def __init__(self, output: str) -> None:
        self.output = output

    def write_parquet(self, data) -> None:
        table = pa.Table.from_pandas(pd.DataFrame(data))
        pq.write_table(table, os.path.join(base_dir, self.output))
