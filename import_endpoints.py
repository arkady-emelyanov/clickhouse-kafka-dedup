import glob
import pandas as pd
from clickhouse_driver import Client

EVENTS_DIR="./data/endpoint_metadata"

client = Client(
    host="localhost",
    user="default",
    password="password123",
    settings={'use_numpy': True}
)
total_appended = 0
files = glob.glob(f"{EVENTS_DIR}/*.parquet")
files_processed = 0

print(f"Found {len(files)} files, uploading the data...")
for file in files:
    print(f"{file}: opening the file...")
    df = pd.read_parquet(file)

    num_inserted = client.insert_dataframe('INSERT INTO devices VALUES', df)
    if not num_inserted:
        print(f"{file}: no rows inserted!")
    else:
        print(f"{file}: inserted {num_inserted} rows")
        total_appended += num_inserted
    files_processed += 1

# Done
print(f"Upload done!")
