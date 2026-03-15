# Slicks

[![CI](https://github.com/Western-Formula-Racing/wfr-telemetry/actions/workflows/ci.yml/badge.svg)](https://github.com/Western-Formula-Racing/wfr-telemetry/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/Western-Formula-Racing/wfr-telemetry/branch/main/graph/badge.svg)](https://codecov.io/gh/Western-Formula-Racing/wfr-telemetry)
[![interrogate](https://raw.githubusercontent.com/Western-Formula-Racing/wfr-telemetry/main/interrogate_badge.svg)](https://interrogate.readthedocs.io/)

The home baked data pipeline for **Western Formula Racing**.

This package handles:
1. **Data Ingestion:** Reliable fetching from InfluxDB 3.0 with adaptive chunking.
2. **Movement Detection:** Smart filtering of "Moving" vs "Idle" car states.
3. **Sensor Discovery:** Tools to explore available sensors on any given race day.
4. **Battery Analysis:** Cell-level voltage statistics, weak cell identification, and pack health monitoring.
5. **Vehicle Dynamics:** G-force calculation and speed estimation from RPM data.

## Documentation

- **[Full Documentation](https://western-formula-racing.github.io/wfr-telemetry/)**
- **[Getting Started](docs/getting_started.md):** Installation and your first script.
- **[API Reference](docs/api_reference.md):** Detailed function documentation.
- **[Advanced Usage](docs/advanced_usage.md):** Configuration, Discovery, and Bulk Exports.

## Installation

Now available on PyPI!

```bash
pip install slicks
```

## Quick Example

```python
import slicks
from datetime import datetime

# 1. Connect (Auto-configured or custom)
slicks.connect_influxdb3(db="WFR25")

# 2. Fetch Data (One-liner)
df = slicks.fetch_telemetry(
    datetime(2025, 9, 28), 
    datetime(2025, 9, 30), 
    "INV_Motor_Speed"
)

print(df.describe())
```

See [Getting Started](docs/getting_started.md) for more details.
