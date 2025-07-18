# Marimo Reactive Notebook Development Loop

## Quick Reference Commands

```bash
# Kill existing processes
pkill -f marimo

# Start live edit server  
nohup uv run marimo edit front_modeling.py --watch --port 8080 > marimo_server.log 2>&1 &

# Check server status
tail -5 marimo_server.log  # Look for URL with access token

# Export current state
uv run marimo export html front_modeling.py -o current_output.html

# Verify changes
grep "Total facilities:" current_output.html
```

## Full Development Loop

1. **Kill & Start**: `pkill -f marimo` → start server → check logs for URL
2. **Export Baseline**: Export HTML and inspect computed values
3. **Edit File**: Use Edit tool to modify notebook cells
4. **Export & Verify**: Re-export HTML and confirm changes propagated

## Key Insights

- **Reactive Updates**: Changes to data automatically recompute dependent cells
- **Edit Tool Works**: File modifications via Edit tool trigger marimo's file watcher
- **HTML Export**: Static exports contain computed session data for easy parsing
- **Server URL**: Format is `http://localhost:8080?access_token=<token>`

## Project Context

Environmental regulatory data analysis with EPA datasets (RCRA, ECHO, ICIS-AIR, NPDES). DuckDB storage, Plotly visualizations, reactive dashboards.