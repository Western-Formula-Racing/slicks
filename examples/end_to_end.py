import slicks as wfr
import matplotlib.pyplot as plt
from datetime import datetime
import pandas as pd
import sys
import os

def main():
    # ---------------------------------------------------------
    # 1. Configuration & Connection
    # ---------------------------------------------------------
    print("Connecting to Slicks Telemetry Database...")

    # Configure connection. 
    # For CI, these are pulled from environment variables (GitHub Secrets).
    # For local use, you can set these in a .env file or call configure() directly.
    wfr.configure(
        url=os.getenv("INFLUX_URL"),
        token=os.getenv("INFLUX_TOKEN"),
        org=os.getenv("INFLUX_ORG"),
        db=os.getenv("INFLUX_DB")
    )

    # ---------------------------------------------------------
    # 2. Discover Sensors (Optional but good practice)
    # ---------------------------------------------------------
    # Use a time range we know has data (Sept 28 20:20 - 21:00)
    start_time = datetime(2025, 9, 28, 20, 20, 0)
    end_time   = datetime(2025, 9, 28, 21, 0, 0)

    print(f"Scanning for sensors between {start_time} and {end_time}...")
    # discover_sensors might print to stdout, which is fine
    available = wfr.discover_sensors(start_time, end_time)
    
    # ---------------------------------------------------------
    # 3. Fetch Data
    # ---------------------------------------------------------
    target_signals = ["INV_Motor_Speed", "INV_DC_Bus_Current"]
    print(f"Fetching data for: {target_signals}...")

    # Fetch 1-second resampled data
    df = wfr.fetch_telemetry(start_time, end_time, signals=target_signals, filter_movement=False)

    if df is None or df.empty:
        print("No data found! CI test might fail if this persists.")
        sys.exit(1)

    print(f"Successfully loaded {len(df)} data points.")

    # ---------------------------------------------------------
    # 4. Visualization
    # ---------------------------------------------------------
    print("Generating plot...")
    wfr_purple = '#4F2683'

    fig, ax1 = plt.subplots(figsize=(10, 6))

    # Plot Motor Speed on Left Axis
    color_speed = wfr_purple
    ax1.set_xlabel('Time')
    ax1.set_ylabel('Motor Speed (RPM)', color=color_speed, fontsize=12)
    ax1.plot(df.index, df['INV_Motor_Speed'], color=color_speed, label='Motor Speed', linewidth=1.2)
    ax1.tick_params(axis='y', labelcolor=color_speed)
    ax1.grid(True, alpha=0.3)

    # Create a second y-axis for Current
    ax2 = ax1.twinx()
    color_current = 'tab:orange'
    ax2.set_ylabel('Inverter DC Bus Current (Amps)', color=color_current, fontsize=12)
    ax2.plot(df.index, df['INV_DC_Bus_Current'], color=color_current, label='DC Current', linewidth=1.2, alpha=0.8)
    ax2.tick_params(axis='y', labelcolor=color_current)

    plt.title(f'Telemetry Analysis: Motor Speed vs Inverter Current\n{start_time.date()}', fontsize=14, fontweight='bold')
    fig.tight_layout()

    # In CI/Test mode, we don't want to block on show()
    # We check if we are running in a CI environment or just want to save
    if os.getenv("CI") or os.getenv("TEST_MODE"):
        output_path = "ci_plot_output.png"
        plt.savefig(output_path)
        print(f"Plot saved to {output_path} (CI Mode)")
    else:
        plt.show()

if __name__ == "__main__":
    main()
