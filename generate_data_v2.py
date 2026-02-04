import random
import pandas as pd
import numpy as np
import uuid
import os
import shutil
from datetime import datetime, timedelta
from multiprocessing import Pool, cpu_count


# ---------------- Configuration ----------------
DATA_DIR = "./data"
ENDPOINT_METADATA = "endpoint_metadata"
ENDPOINT_EVENTS = "endpoint_events"

CORES = cpu_count() - 1 if cpu_count() > 1 else 1
NUM_ENDPOINTS = 6_000_000
SIM_DAYS = 10

ENDPOINT_CHATTY = "chatty"
ENDPOINT_STABLE = "stable"

P_ALERT_CHATTY = 0.001736
P_ALERT_STABLE = 0.0001
P_TO_OK = 0.02
P_CHANGE_SEV = 0.05

STATUS_OK = "OK"
STATUS_ALERTING = "ALERTING"
SEVERITIES = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
DEVICE_VENDORS = ["VendorA", "VendorB", "VendorC", "VendorD"]
DEVICE_FIRMWARE_VERSIONS = {
    "VendorA": ["v1.0.0", "v1.1.0", "v2.0.1", "v2.1.1", "v3.0.3"],
    "VendorB": ["v1.0.0", "v1.2.0"],
    "VendorC": ["v1.1.0", "v1.3.0", "v2.1.0", "v2.3.1", "v3.2.0"],
    "VendorD": ["v1.2.0", "v1.4.0", "v2.2.6"],
}
EVENT_TYPES = {
    "VendorA": ["TypeA1", "TypeA2", "TypeA3"],
    "VendorB": ["TypeB1", "TypeB2"],
    "VendorC": ["TypeC1", "TypeC2", "TypeC3", "TypeC4"],
    "VendorD": ["TypeD1", "TypeD2"],
}

SIM_START = datetime(2026, 1, 1)
SIM_END = SIM_START + timedelta(days=SIM_DAYS)


# Class
class Event:
    def __init__(self, device_id: int):
        self.updated_at = None
        self.device_id = device_id
        self.event_id = None
        self.event_type = None
        self.severity = None
        self.status = STATUS_OK
        self.start_time = None
        self.end_time = None

    def start(self, tt: datetime, event_type: str):
        self.updated_at = tt
        self.event_id = uuid.uuid4()
        self.event_type = event_type
        self.severity = random.choice(SEVERITIES)
        self.status = STATUS_ALERTING
        self.start_time = tt
        self.end_time = None

    def change_severity(self, tt: datetime):
        self.updated_at = tt
        self.severity = random.choice(SEVERITIES)

    def resolve(self, tt):
        self.updated_at = tt
        self.status = STATUS_OK
        self.end_time = tt

    def to_dict(self) -> dict:
        return {
            "EventID": str(self.event_id),
            "DeviceID": self.device_id,
            "EventType": self.event_type,
            "Severity": self.severity,
            "Status": self.status,
            "StartTime": self.start_time,
            "EndTime": self.end_time,
            "UpdatedAt": self.updated_at,
        }


class Endpoint:
    def __init__(self, device_id: int, profile: str):
        self.device_id = device_id
        self.profile = profile
        self.vendor = random.choice(DEVICE_VENDORS)
        self.firmware_version = random.choice(DEVICE_FIRMWARE_VERSIONS[self.vendor])
        self.manufacture_date = SIM_START - timedelta(days=random.randint(30, 3650))


# Utilities
def cleanup():
    if os.path.exists(DATA_DIR):
        shutil.rmtree(DATA_DIR)
    
    os.makedirs(os.path.join(DATA_DIR, ENDPOINT_METADATA))
    os.makedirs(os.path.join(DATA_DIR, ENDPOINT_EVENTS))


# Write endpoint metadata
def write_endpoint_metadata(endpoint_file: str, endpoint_data: dict):
    endpoint_meta_rows = []
    for endpoint in endpoint_data.values():
        endpoint_meta_rows.append({
            "DeviceID": endpoint.device_id,
            "Vendor": endpoint.vendor,
            "FirmwareVersion": endpoint.firmware_version,
            "ManufactureDate": endpoint.manufacture_date,
            "UpdatedAt": SIM_START,
        })
    df = pd.DataFrame(endpoint_meta_rows)
    df.to_parquet(endpoint_file, index=False)


# Event simulation worker
def simulation_worker(chunk_id: int, endpoints_dict: dict, endpoints_chunk: list[int]):    
    rng = np.random.default_rng(seed=random.randint(0, 1000000))
    event_rows = []

    print(f"Chunk {chunk_id} starting: {len(endpoints_chunk)} endpoints")
    for device_id in endpoints_chunk:
        endpoint = endpoints_dict[device_id]

        profile = endpoint.profile # chatty or stable
        p_alert = P_ALERT_CHATTY if profile == ENDPOINT_CHATTY else P_ALERT_STABLE # probability of alert
        event = Event(device_id=device_id) # event instance

        tt = SIM_START
        while tt < SIM_END:
            
            if event.status == STATUS_OK:
                # OK -> ALERTING
                wait_time_to_alert = np.random.geometric(p_alert)
                tt = tt + timedelta(minutes=int(wait_time_to_alert))
                if tt >= SIM_END:
                    break

                event_type = rng.choice(EVENT_TYPES[endpoint.vendor])
                event.start(tt, event_type)
                event_rows.append(event.to_dict())

            else:
                wait_time_to_ok = rng.geometric(P_TO_OK)
                wait_time_to_severity = rng.geometric(P_CHANGE_SEV)

                if wait_time_to_severity < wait_time_to_ok:
                    # ALERTING -> ALERTING (change severity)
                    tt = tt + timedelta(minutes=int(wait_time_to_severity))
                    if tt >= SIM_END:
                        break                    
                    event.change_severity(tt)
                    event_rows.append(event.to_dict())

                else:
                    # ALERTING -> OK
                    tt = tt + timedelta(minutes=int(wait_time_to_ok))
                    if tt >= SIM_END:
                        break
                    event.resolve(tt)
                    event_rows.append(event.to_dict())

    # Write to parquet per chunk
    print(f"Chunk {chunk_id} writing {len(event_rows)} events to parquet...")
    df_chunk = pd.DataFrame(event_rows)
    chunk_file = os.path.join(DATA_DIR, ENDPOINT_EVENTS, f"events_{chunk_id}.parquet")
    df_chunk.to_parquet(chunk_file, index=False)
    print(f"Chunk {chunk_id} completed.")


if __name__ == "__main__":
    random.seed(datetime.now().timestamp())
    cleanup()

    endpoint_data = {}
    endpoint_ids = []

    # Generate endpoints and store them as parquet
    print("Generating endpoints...")
    for i in range(NUM_ENDPOINTS):
        device_id = i + 1
        profile = ENDPOINT_CHATTY if random.random() < 0.3 else ENDPOINT_STABLE

        endpoint_ids.append(device_id)
        endpoint_data[device_id] = Endpoint(
            device_id=device_id,
            profile=profile
        )
    print(f"Generated: {NUM_ENDPOINTS} endpoints.")
    print("Writing endpoint metadata...")
    write_endpoint_metadata(
        os.path.join(DATA_DIR, ENDPOINT_METADATA, "metadata.parquet"),
        endpoint_data,
    )

    # Run simulation
    endpoints_per_chunk = NUM_ENDPOINTS // CORES
    with Pool(processes=CORES) as pool:
        print(f"Starting simulation of {NUM_ENDPOINTS} endpoints...")
        tasks = []
        for i in range(CORES):
            start_idx = i * endpoints_per_chunk
            if i == CORES - 1:
                end_idx = NUM_ENDPOINTS
            else:
                end_idx = start_idx + endpoints_per_chunk
            endpoint_chunk = endpoint_ids[start_idx:end_idx]
            tasks.append(pool.apply_async(simulation_worker, args=(i, endpoint_data, endpoint_chunk)))

        print(f"Waiting for {len(tasks)} chunks to complete...")
        for task in tasks:
            task.get()
    
