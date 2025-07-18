---
title: Snapshot
marimo-version: 0.14.10
width: medium
---

```python {.marimo}
import marimo as mo
```

# Environmental Data Analysis Dashboard

This notebook provides analysis of EPA regulatory data including:
- Air quality monitoring (ICIS-AIR)
- Water discharge permits (NPDES)
- Hazardous waste tracking (RCRA)
- Facility compliance data

```python {.marimo}
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# Sample data for demonstration - TESTING REACTIVE UPDATES v5
sample_data = pd.DataFrame({
    'facility_type': ['Air Quality', 'Water', 'Hazardous Waste', 'Mixed'],
    'count': [120, 140, 65, 45],
    'violations': [25, 38, 20, 15]
})

mo.md(f"## Current Data Overview\n\nTotal facilities: {sample_data['count'].sum()}\n\nLast updated: 1:50 PM - INSPECT WEBSOCKET MESSAGES!")
```

```python {.marimo}
# Create a bar chart of facility types
fig = px.bar(sample_data, x='facility_type', y='count', 
             title='Facilities by Type',
             color='violations',
             color_continuous_scale='Reds')

mo.ui.plotly(fig)
```

```python {.marimo}
# Add a violations chart
violations_fig = px.pie(sample_data, names='facility_type', values='violations',
                       title='Violations Distribution by Facility Type')

mo.ui.plotly(violations_fig)
```

```python {.marimo}
# Add a summary statistics table
summary_stats = {
    'Total Facilities': sample_data['count'].sum(),
    'Total Violations': sample_data['violations'].sum(),
    'Average Violations per Facility': round(sample_data['violations'].sum() / sample_data['count'].sum(), 2),
    'Highest Risk Facility Type': sample_data.loc[sample_data['violations'].idxmax(), 'facility_type']
}

mo.md(f"""
## Summary Statistics

- **Total Facilities**: {summary_stats['Total Facilities']}
- **Total Violations**: {summary_stats['Total Violations']}
- **Average Violations per Facility**: {summary_stats['Average Violations per Facility']}
- **Highest Risk Facility Type**: {summary_stats['Highest Risk Facility Type']}
""")
```