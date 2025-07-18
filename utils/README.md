# Marimo Development Utils

This directory contains utilities for working with marimo reactive notebooks.

## Files

### `marimo_parser.py`
Enhanced marimo HTML export parser using BeautifulSoup for robust JSON extraction.

**Features:**
- Clean HTML parsing with BeautifulSoup
- Extract cell name → function name mappings
- Capture console output (stdout/stderr, warnings, errors)
- Extract computed values from outputs
- Detect interactive plots and rich outputs

**Usage:**
```bash
# As a command (recommended)
uv run marimo-dev exported_notebook.html --verbose

# Or directly
uv run python3 utils/marimo_parser.py exported_notebook.html --verbose
```

### `marimo_parser_demo.py`
Working demonstration of the marimo parser capabilities.

**Usage:**
```bash
# As a command (recommended)
uv run marimo-demo exported_notebook.html

# Or directly
uv run python3 utils/marimo_parser_demo.py exported_notebook.html
```

### `CLAUDE.md`
Development loop documentation for marimo reactive notebooks.

## Available Scripts

### Marimo Development Utilities
- `uv run marimo-dev <html_file> [--verbose]` - Parse marimo exports
- `uv run marimo-demo <html_file>` - Demo parser capabilities

### Marimo Workflow Commands (from CLAUDE.md)
- `uv run marimo-kill` - Kill existing marimo processes
- `uv run marimo-start [notebook.py] [--port 8080]` - Start live edit server (logs to `logs/`)
- `uv run marimo-export [notebook.py]` - Export to timestamped HTML in `logs/`
- `uv run marimo-logs` - Check recent server logs
- `uv run marimo-verify` - Verify latest export and run parser
- `uv run marimo-loop` - Complete development cycle (kill → start → export → verify)

### Example Workflow
```bash
# Quick development loop
uv run marimo-loop

# Or step by step:
uv run marimo-kill
uv run marimo-start front_modeling.py
uv run marimo-export
uv run marimo-verify
```

### Files and Logs
- **Server logs**: `logs/marimo_server.log`
- **HTML exports**: `logs/notebook_YYYYMMDD_HHMMSS.html`
- **All outputs**: Organized in `logs/` directory

## Key Insights

1. **Cell Name Mapping**: The `"name"` field in notebook cells maps to function names in the Python file
2. **Console Output**: All prints, warnings, and errors are captured in the session data
3. **Computed Values**: Results like "Total facilities: 475" can be extracted from output HTML
4. **Reactive Updates**: File changes trigger automatic re-execution when configured properly
5. **Export Structure**: Clean JSON structure embedded in script tags, parseable with BeautifulSoup

## Development Workflow

1. Edit notebook file with Edit tool
2. Export HTML to capture current state
3. Parse with marimo_parser.py to extract data
4. Use extracted data for analysis/monitoring