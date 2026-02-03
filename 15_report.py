import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import glob
import os
import io
import base64

# --- Configuration ---
DATA_DIR = "./data"
REPORT_FILE = "sensor_report.html"

def fig_to_base64(fig):
    """Converts a matplotlib figure to a base64 string for HTML embedding."""
    img_buffer = io.BytesIO()
    fig.savefig(img_buffer, format='png', bbox_inches='tight')
    img_buffer.seek(0)
    img_str = base64.b64encode(img_buffer.read()).decode('utf-8')
    return f"data:image/png;base64,{img_str}"

def load_data():
    files = glob.glob(f"{DATA_DIR}/*.parquet")
    if not files:
        print("No parquet files found. Check your generation script output.")
        return None
    print(f"Loading {len(files)} daily batches...")
    return pd.concat([pd.read_parquet(f) for f in files])

def count_alert_cycles(df):
    """
    Counts transitions: OK -> ALERTING -> OK.
    This identifies rows where a sensor returned to 'OK' after an alert.
    """
    # A completed cycle is defined by an 'OK' status that has 
    # both a StartTime and an EndTime (the duration of the alert).
    completed_cycles = df[
        (df['Status'] == 'OK') & 
        (df['StartTime'].notna()) & 
        (df['EndTime'].notna())
    ]
    
    return len(completed_cycles)

def generate_report():
    df = load_data()
    if df is None: 
        return
    print("Data loaded, generating visualizations...")

    # Ensure Timestamps are standardized
    df['StartTime'] = pd.to_datetime(df['StartTime'])
    df['EndTime'] = pd.to_datetime(df['EndTime'])

    completed_cycles = count_alert_cycles(df)

    # 1. Alert Frequency
    alerting_df = df[df['Status'] == 'ALERTING'].copy()
    fig1, ax1 = plt.subplots(figsize=(12, 6))
    if not alerting_df.empty:
        trend = alerting_df.set_index('StartTime').resample('1h').size()
        trend.plot(kind='line', color='#d9534f', linewidth=1.5, ax=ax1)
    ax1.set_title('Total Number of Alert Events (resampled hourly)')
    ax1.set_ylabel('Number of Alert Events')
    ax1.grid(True, alpha=0.3)
    alert_img_base64 = fig_to_base64(fig1)
    plt.close(fig1)

    # 2. Duration Distribution
    ok_df = df[df['Status'] == 'OK'].dropna(subset=['StartTime', 'EndTime']).copy()
    fig2, ax2 = plt.subplots(figsize=(12, 6))
    if not ok_df.empty:
        ok_df['DurMin'] = (ok_df['EndTime'] - ok_df['StartTime']).dt.total_seconds() / 60
        plot_data = ok_df['DurMin']
        if len(plot_data) > 100000:
            plot_data = plot_data.sample(100000)
        sns.histplot(plot_data, bins=50, kde=True, color='#5bc0de', ax=ax2)
    ax2.set_title('Alert Duration Distribution')
    ax2.set_xlabel('Minutes')
    duration_img_base64 = fig_to_base64(fig2)
    plt.close(fig2)

    # 3. Severity Total Counts
    fig3, ax3 = plt.subplots(figsize=(10, 6))
    severity_order = ['LOW', 'MEDIUM', 'HIGH', 'CRITICAL']
    sev_counts = alerting_df['Severity'].value_counts().reindex(severity_order).fillna(0).astype(int)
    
    sns.barplot(x=sev_counts.index, hue=sev_counts.index, y=sev_counts.values, palette='Reds_r', legend=False, ax=ax3)
    
    for p in ax3.patches:
        ax3.annotate(f'{int(p.get_height()):,}', 
                    (p.get_x() + p.get_width() / 2., p.get_height()), 
                    ha='center', va='center', xytext=(0, 9), 
                    textcoords='offset points', fontsize=11, fontweight='bold')
    
    ax3.set_title('Total Number of Alerts by Severity')
    ax3.set_ylabel('Event Count')
    severity_img_base64 = fig_to_base64(fig3)
    plt.close(fig3)    

    # --- HTML with Table Generation ---
    severity_rows_html = "".join([
        f"<tr><td><strong>{sev}</strong></td><td>{count:,}</td></tr>" 
        for sev, count in sev_counts.items()
    ])

    # 4. Completed Alert Cycles Over Time
    completed_cycles_df = df[
        (df['Status'] == 'OK') & 
        (df['StartTime'].notna())
    ].copy()

    fig4, ax4 = plt.subplots(figsize=(12, 6))
    if not completed_cycles_df.empty:
        # Resample by hour based on when the alert ended
        cycles_trend = completed_cycles_df.set_index('EndTime').resample('1h').size()
        
        cycles_trend.plot(kind='line', color='#5cb85c', linewidth=2, ax=ax4)
        ax4.fill_between(cycles_trend.index, cycles_trend.values, color='#5cb85c', alpha=0.2)
        
    ax4.set_title('Completed Alert Cycles Over Time (OK → ALERTING → OK)')
    ax4.set_ylabel('Number of Resolved Alerts')
    ax4.set_xlabel('Time')
    ax4.grid(True, alpha=0.3)
    
    cycles_img_base64 = fig_to_base64(fig4)
    plt.close(fig4)

    # --- HTML Generation ---
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
            img {{ width: 100%; max-width: 900px; border-radius: 4px; }}
            table {{ margin: 20px auto; border-collapse: collapse; width: 50%; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #f2f2f2; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Sensor Simulation Results</h1>
            
            <div class="stats-bar">
                <div class="stat-card"><span class="stat-val">{len(df):,}</span> Total state changes</div>
                <div class="stat-card"><span class="stat-val">{len(alerting_df):,}</span> Total number of events in ALERTING state</div>
                <div class="stat-card"><span class="stat-val">{completed_cycles:,}</span> Completed alert cycles</div>
            </div>
            
            <div class="metric-box">
                <img src="{alert_img_base64}" />
            </div>

            <div class="metric-box">
                <img src="{duration_img_base64}" />
            </div>

            <div class="metric-box">
                <img src="{cycles_img_base64}" />
            </div>

            <div class="metric-box">
                <h2>Total Alerts per Severity</h2>
                <table>
                    <thead><tr><th>Severity Level</th><th>Total Count</th></tr></thead>
                    <tbody>{severity_rows_html}</tbody>
                </table>
                <img src="{severity_img_base64}" />
            </div>
        </div>
    </body>
    </html>
    """

    with open(REPORT_FILE, "w") as f:
        f.write(html_content)
    
    print(f"Success! Report generated: {os.path.abspath(REPORT_FILE)}")

if __name__ == "__main__":
    generate_report()