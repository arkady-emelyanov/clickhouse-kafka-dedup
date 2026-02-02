import pandas as pd
import numpy as np
import uuid
import os
import shutil
from datetime import datetime, timedelta
from multiprocessing import Pool, cpu_count

# --- Configuration ---
DATA_DIR = "./data"
CHECKPOINT_DIR = "./checkpoints"
NUM_SENSORS = 500_000
SIM_WEEKS = 1
TICKS = SIM_WEEKS * 7 * 24 * 60
CORES = cpu_count()-1 if cpu_count() > 1 else 1

def cleanup():
    for path in [DATA_DIR, CHECKPOINT_DIR]:
        if os.path.exists(path):
            shutil.rmtree(path)
        os.makedirs(path)

def run_sensor_chunk(chunk_id, num_sensors_in_chunk):    
    """Worker function for a subset of sensors."""
    print(f"Starting chunk {chunk_id} with {num_sensors_in_chunk} sensors.")

    # Initialize subset state
    state_df = pd.DataFrame({
        'SensorID': [str(uuid.uuid4()) for _ in range(num_sensors_in_chunk)],
        'StartTime': pd.NaT,
        'EndTime': pd.NaT,
        'Status': 'OK',
        'Severity': None
    })
    
    severities = ['LOW', 'HIGH', 'WARNING', 'CRITICAL']
    current_sim_time = datetime(2026, 1, 1)
    batch_buffer = []

    for tick in range(TICKS):
        current_sim_time += timedelta(minutes=1)
        
        # State Masks
        mask_ok = state_df['Status'] == 'OK'
        mask_alerting = state_df['Status'] == 'ALERTING'
        
        # Probabilities
        to_alert = (np.random.rand(num_sensors_in_chunk) < 0.0017) & mask_ok
        to_ok = (np.random.rand(num_sensors_in_chunk) < 0.02) & mask_alerting
        to_change_sev = (np.random.rand(num_sensors_in_chunk) < 0.05) & mask_alerting & (~to_ok)

        changed_indices = []

        if to_alert.any():
            idx = state_df.index[to_alert]
            state_df.loc[idx, ['Status', 'StartTime', 'EndTime']] = ['ALERTING', current_sim_time, pd.NaT]
            state_df.loc[idx, 'Severity'] = np.random.choice(severities, len(idx))
            changed_indices.extend(idx)

        if to_ok.any():
            idx = state_df.index[to_ok]
            state_df.loc[idx, ['Status', 'EndTime', 'Severity']] = ['OK', current_sim_time, None]
            changed_indices.extend(idx)

        if to_change_sev.any():
            idx = state_df.index[to_change_sev]
            state_df.loc[idx, 'Severity'] = np.random.choice(severities, len(idx))
            changed_indices.extend(idx)

        # Buffer changes
        if changed_indices:
            batch_buffer.append(state_df.iloc[list(set(changed_indices))].copy())

        # Materialize every 360 ticks (6 hours) to keep files decently sized
        if tick % 1440 == 0 and batch_buffer:
            output_df = pd.concat(batch_buffer)
            file_name = f"{DATA_DIR}/chunk_{chunk_id}_tick_{tick}.parquet"
            output_df.to_parquet(file_name, engine='pyarrow', index=False)
            batch_buffer = []

    return f"Chunk {chunk_id} complete."

if __name__ == "__main__":
    cleanup()
    
    # Calculate sensors per process
    sensors_per_core = NUM_SENSORS // CORES
    tasks = [(i, sensors_per_core) for i in range(CORES)]
    
    print(f"Starting parallel generation with {CORES} processes...")
    print(f"Simulating {NUM_SENSORS} sensors for {SIM_WEEKS} weeks.")

    

    with Pool(processes=CORES) as pool:
        results = pool.starmap(run_sensor_chunk, tasks)
        
    for res in results:
        print(res)
    
    print("All sensor data generated successfully.")
