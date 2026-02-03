import pandas as pd
import numpy as np
import uuid
import os
import shutil
from datetime import datetime, timedelta
from multiprocessing import Pool, cpu_count

# ---------------- Configuration ----------------

DATA_DIR = "./data"
NUM_SENSORS = 5_000_000
SIM_DAYS = 10
MINUTES_PER_DAY = 1440
SIM_MINUTES = SIM_DAYS * MINUTES_PER_DAY

CORES = cpu_count() - 1 if cpu_count() > 1 else 1

SENSOR_PROFILE_CHATTY = "chatty"
SENSOR_PROFILE_STABLE = "stable"

P_ALERT_CHATTY = 0.001736
P_ALERT_STABLE = 0.0001
P_TO_OK = 0.02
P_CHANGE_SEV = 0.05

SEVERITIES = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]

SIM_START = datetime(2026, 1, 1)
SIM_END = SIM_START + timedelta(minutes=SIM_MINUTES)

# ---------------- Utilities ----------------

def cleanup():
    if os.path.exists(DATA_DIR):
        shutil.rmtree(DATA_DIR)
    os.makedirs(DATA_DIR)


def day_index(ts):
    d = int((ts - SIM_START).total_seconds() // 60) // MINUTES_PER_DAY
    if d < 0:
        return 0
    if d >= SIM_DAYS:
        return SIM_DAYS - 1
    return d


# ---------------- Worker ----------------

def run_sensor_chunk(chunk_id, num_sensors):
    rng = np.random.default_rng(seed=42 + chunk_id)

    sensor_ids = [str(uuid.uuid4()) for _ in range(num_sensors)]
    profiles = rng.choice(
        [SENSOR_PROFILE_CHATTY, SENSOR_PROFILE_STABLE],
        size=num_sensors,
        p=[0.30, 0.70],
    )

    day_buffers = {d: [] for d in range(SIM_DAYS)}

    def emit(row):
        d = day_index(row["UpdatedAt"])
        day_buffers[d].append(row)

    print(f"Chunk {chunk_id} starting: {num_sensors} sensors")
    for i in range(num_sensors):
        sensor_id = sensor_ids[i]
        profile = profiles[i]
        p_alert = P_ALERT_CHATTY if profile == SENSOR_PROFILE_CHATTY else P_ALERT_STABLE

        state = "OK"
        alert_start = None
        current_sev = None
        t = SIM_START

        while t < SIM_END:

            if state == "OK":
                wait = rng.geometric(p_alert)
                t = t + timedelta(minutes=int(wait))
                if t >= SIM_END:
                    break

                # OK → ALERTING
                state = "ALERTING"
                alert_start = t
                current_sev = rng.choice(SEVERITIES)

                emit({
                    "SensorID": sensor_id,
                    "StartTime": alert_start,
                    "EndTime": pd.NaT,
                    "Status": "ALERTING",
                    "Severity": current_sev,
                    "UpdatedAt": t,
                })

            else:  # ALERTING
                wait_ok = rng.geometric(P_TO_OK)
                wait_sev = rng.geometric(P_CHANGE_SEV)

                if wait_sev < wait_ok:
                    # severity change
                    t = t + timedelta(minutes=int(wait_sev))
                    if t >= SIM_END:
                        break

                    current_sev = rng.choice(SEVERITIES)

                    emit({
                        "SensorID": sensor_id,
                        "StartTime": alert_start,
                        "EndTime": pd.NaT,
                        "Status": "ALERTING",
                        "Severity": current_sev,
                        "UpdatedAt": t,
                    })

                else:
                    # ALERTING → OK
                    t = t + timedelta(minutes=int(wait_ok))
                    if t >= SIM_END:
                        break

                    emit({
                        "SensorID": sensor_id,
                        "StartTime": alert_start,
                        "EndTime": t,
                        "Status": "OK",
                        "Severity": None,
                        "UpdatedAt": t,
                    })

                    state = "OK"
                    alert_start = None
                    current_sev = None

    # write parquet
    print(f"Chunk {chunk_id} writing parquet data")
    for d, rows in day_buffers.items():
        if not rows:
            continue
        df = pd.DataFrame(rows)
        df = df[
            ["SensorID", "StartTime", "EndTime", "Status", "Severity", "UpdatedAt"]
        ]
        path = f"{DATA_DIR}/chunk_{chunk_id}_day_{d}.parquet"
        df.to_parquet(path, engine="pyarrow", index=False, compression="snappy")
        print(f"Chunk {chunk_id} wrote day {d}: {len(df)} rows")

    return f"Chunk {chunk_id} done"


# ---------------- Main ----------------

if __name__ == "__main__":
    cleanup()

    sensors_per_core = NUM_SENSORS // CORES
    tasks = [(i, sensors_per_core) for i in range(CORES)]

    print(f"Running {CORES} workers")
    print(f"Simulating {NUM_SENSORS} sensors for {SIM_DAYS} days")

    with Pool(CORES) as pool:
        results = pool.starmap(run_sensor_chunk, tasks)

    for r in results:
        print(r)

    print("✅ All data generated")
