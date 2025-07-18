# Marimo Reactive Notebook Development Loop

## Quick Reference Commands

```bash
# Notebook Management:
just set notebook.py               # Set current notebook for all operations
just current                       # Show currently set notebook

# Development Commands:
just kill                          # Kill existing processes
just start                         # Start live server (uses set notebook)
just export                        # Export to logs/notebook.html (no timestamp)
just logs                          # Check recent server logs
just verify                        # AI-powered analysis with Gemini Flash (fast)
just verify-pro                    # Detailed analysis with Gemini Pro (thorough)
just loop                          # Full cycle: kill → start → export → verify
just help                          # Show all available commands
```

## Development Workflow

### Setup Phase:
1. **Set Notebook**: `just set front_modeling.py` - Configure target notebook once
2. **Check Status**: `just current` - Verify which notebook is active

### Rapid Iteration Mode:
1. **Quick Start**: `just loop` - Full cycle with current notebook
2. **Edit & Export**: Modify files → `just export` → `just verify`
3. **Deep Review**: `just verify-pro` for detailed AI analysis
4. **Check Status**: `just logs` to see server health and access token

### AI-Powered Verification:
- **`just verify`**: Fast feedback using Gemini Flash - perfect for quick iteration
- **`just verify-pro`**: Comprehensive analysis using Gemini Pro - ideal for thorough reviews
- Both analyze: data quality, visualization effectiveness, code issues, and improvement suggestions

### Smart Context Management:
- Notebook setting persists across command invocations
- All commands automatically use the configured notebook
- No need to repeatedly specify notebook names

## Key Insights

- **Reactive Updates**: Changes to data automatically recompute dependent cells
- **Edit Tool Works**: File modifications via Edit tool trigger marimo's file watcher
- **HTML Export**: Static exports contain computed session data for easy parsing
- **Server URL**: Format is `http://localhost:8080?access_token=<token>`

## Project Context

Environmental regulatory data analysis with EPA datasets (RCRA, ECHO, ICIS-AIR, NPDES). DuckDB storage, Plotly visualizations, reactive dashboards.