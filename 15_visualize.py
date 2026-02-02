import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import glob
import os

# --- Configuration ---
DATA_DIR = "./data"
REPORT_FILE = "sensor_report.html"  # Now in root folder

def load_data():
    files = glob.glob(f"{DATA_DIR}/*.parquet")
    if not files:
        print("No parquet files found. Check your generation script output.")
        return None
    print(f"Loading {len(files)} daily batches...")
    return pd.concat([pd.read_parquet(f) for f in files])

def generate_report():
    df = load_data()
    if df is None: 
        return

    # Ensure Timestamps are standardized
    df['StartTime'] = pd.to_datetime(df['StartTime'])
    df['EndTime'] = pd.to_datetime(df['EndTime'])

    # Image filenames (local to DATA_DIR for saving, relative for HTML)
    alert_img_name = "alert_trend.png"
    duration_img_name = "alert_duration.png"
    severity_img_name = "severity_dist.png"

    # 1. Alert Frequency - Resampled to Hourly
    alerting_df = df[df['Status'] == 'ALERTING'].copy()
    plt.figure(figsize=(12, 6))
    if not alerting_df.empty:
        trend = alerting_df.set_index('StartTime').resample('1h').size()
        trend.plot(kind='line', color='#d9534f', linewidth=1.5)
    plt.title('System-Wide Alert Frequency (Resampled Hourly)')
    plt.ylabel('New Alert Events')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(DATA_DIR, alert_img_name))
    plt.close()

    # 2. Duration Distribution - Sampled for Performance
    ok_df = df[df['Status'] == 'OK'].dropna(subset=['StartTime', 'EndTime']).copy()
    plt.figure(figsize=(12, 6))
    if not ok_df.empty:
        ok_df['DurMin'] = (ok_df['EndTime'] - ok_df['StartTime']).dt.total_seconds() / 60
        plot_data = ok_df['DurMin']
        if len(plot_data) > 100000:
            plot_data = plot_data.sample(100000)
        sns.histplot(plot_data, bins=50, kde=True, color='#5bc0de')
    plt.title('Alert Duration Distribution (Simulated 3 Weeks)')
    plt.xlabel('Minutes')
    plt.tight_layout()
    plt.savefig(os.path.join(DATA_DIR, duration_img_name))
    plt.close()

    # 3. Severity Distribution
    # plt.figure(figsize=(10, 6))
    # sev_counts = df[df['Severity'].notnull()]['Severity'].value_counts()
    # if not sev_counts.empty:
    #     order = ['LOW', 'WARNING', 'HIGH', 'CRITICAL']
    #     sev_counts = sev_counts.reindex([s for s in order if s in sev_counts.index])
    #     sns.barplot(x=sev_counts.index, y=sev_counts.values, palette='magma')
    # plt.title('Total Severity Distribution')
    # plt.tight_layout()
    # plt.savefig(os.path.join(DATA_DIR, severity_img_name))
    # plt.close()

# 3. Severity Total Counts (Updated)
    plt.figure(figsize=(10, 6))
    severity_order = ['LOW', 'WARNING', 'HIGH', 'CRITICAL']
    # Filter for Alerting rows where severity is set
    sev_counts = alerting_df['Severity'].value_counts().reindex(severity_order).fillna(0).astype(int)
    
    ax = sns.barplot(x=sev_counts.index, hue=sev_counts.index, y=sev_counts.values, palette='Reds_r', legend=False)
    # Add labels on top of bars
    for p in ax.patches:
        ax.annotate(f'{int(p.get_height()):,}', 
                    (p.get_x() + p.get_width() / 2., p.get_height()), 
                    ha = 'center', va = 'center', 
                    xytext = (0, 9), 
                    textcoords = 'offset points',
                    fontsize=11, fontweight='bold')
    
    plt.title('Total Number of Alerts by Severity')
    plt.ylabel('Event Count')
    plt.tight_layout()
    plt.savefig(os.path.join(DATA_DIR, severity_img_name))
    plt.close()

    # --- HTML with Table Generation ---
    severity_rows_html = "".join([
        f"<tr><td><strong>{sev}</strong></td><td>{count:,}</td></tr>" 
        for sev, count in sev_counts.items()
    ])    

    # --- HTML Generation ---
    # Note: We use "data/" prefix for the images because the HTML is in the root.
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Sensor Emulation Report</title>
        <style>
            body {{ font-family: -apple-system, sans-serif; margin: 40px; background: #fafafa; color: #333; }}
            .container {{ max-width: 1100px; margin: auto; background: white; padding: 30px; border-radius: 12px; box-shadow: 0 4px 20px rgba(0,0,0,0.08); }}
            h1 {{ border-bottom: 2px solid #f0f0f0; padding-bottom: 15px; }}
            .stats-bar {{ display: flex; gap: 20px; margin-bottom: 30px; }}
            .stat-card {{ background: #f0f0f0; padding: 15px; border-radius: 6px; flex: 1; text-align: center; }}
            .stat-val {{ font-size: 1.5rem; font-weight: bold; display: block; }}
            .metric-box {{ margin-bottom: 50px; padding: 20px; border: 1px solid #eee; border-radius: 8px; text-align: center; }}
            img {{ width: 100%; max-width: 900px; border-radius: 4px; cursor: zoom-in; }}
            .path-info {{ font-size: 0.8rem; color: #888; margin-top: 5px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Sensor Emulation Dashboard</h1>
            
            <div class="stats-bar">
                <div class="stat-card"><span class="stat-val">{len(df):,}</span> Total Events</div>
                <div class="stat-card"><span class="stat-val">{len(alerting_df):,}</span> Total Alerts</div>
            </div>
            
            <div class="metric-box">
                <h2>Alert Frequency</h2>
                <a href="data/{alert_img_name}" target="_blank"><img src="data/{alert_img_name}" /></a>
                <p class="path-info">Source: data/{alert_img_name}</p>
            </div>

            <div class="metric-box">
                <h2>Duration Distribution</h2>
                <a href="data/{duration_img_name}" target="_blank"><img src="data/{duration_img_name}" /></a>
                <p class="path-info">Source: data/{duration_img_name}</p>
            </div>

            <div class="metric-box">
                <h2>Total Alerts per Severity</h2>
                <table>
                    <thead><tr><th>Severity Level</th><th>Total Count</th></tr></thead>
                    <tbody>{severity_rows_html}</tbody>
                </table>
                <a href="data/{severity_img_name}" target="_blank"><img src="data/{severity_img_name}" /></a>
            </div>
        </div>
    </body>
    </html>
    """

    with open(REPORT_FILE, "w") as f:
        f.write(html_content)
    
    print(f"Success! Report: {os.path.abspath(REPORT_FILE)}")
    print(f"Images kept in: {os.path.abspath(DATA_DIR)}")

if __name__ == "__main__":
    generate_report()
