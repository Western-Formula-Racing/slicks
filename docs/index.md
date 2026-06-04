# Slicks

The home baked data pipeline for **Western Formula Racing**.

This package handles:

1. **Data Ingestion:** Reliable fetching from TimescaleDB 3.0.

2. **Movement Detection:** Smart filtering of "Moving" vs "Idle" car states.

3. **Sensor Discovery:** Tools to explore available sensors on any given race day.

## Documentation

- **[Getting Started](docs/getting_started.md):** Installation and your first script.
- **[API Reference](docs/api_reference.md):** Detailed function documentation.
- **[Advanced Usage](docs/advanced_usage.md):** Configuration, Discovery, and Bulk Exports.

## Installation

```bash
pip install git+https://github.com/Western-Formula-Racing/wfr-telemetry.git
```

## Quick Example

```python
import slicks
from datetime import datetime

# 1. Connect (Auto-configured or custom)
slicks.connect_timescaledb(table="WFR25", dsn="http://postgresdb:9000", "apiv3_your_token")

# 2. Fetch Data (One-liner)
df = slicks.fetch_telemetry(
    datetime(2025, 9, 28), 
    datetime(2025, 9, 30), 
    "INV_Motor_Speed"
)

print(df.describe())
```

See [Getting Started](docs/getting_started.md) for more details.
