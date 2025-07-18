# Marimo CLI Usage Guide

## Setup for Reactive Development with Auto-Run

### 1. Install Dependencies
```bash
uv add watchdog  # For better file watching performance (remove for polling mode)
```

### 2. Create Project Configuration
Add to your `pyproject.toml`:
```toml
[tool.marimo]
[tool.marimo.runtime]
watcher_on_save = "autorun"  # Auto-run cells when file changes
on_cell_change = "autorun"   # Auto-run cells when they change
auto_instantiate = true      # Auto-run cells on startup
auto_reload = "lazy"         # Enable auto-reload

[tool.marimo.server]
follow_symlink = true

[tool.marimo.save]
autosave = "after_delay"
autosave_delay = 1000
format_on_save = false
```

**IMPORTANT**: Also update global config at `~/.config/marimo/marimo.toml`:
```toml
[runtime]
watcher_on_save = "autorun"  # Must match project config
```

### 3. Start Development Server
```bash
# Stop any existing marimo processes
ps aux | grep -E "marimo|python.*marimo" | grep -v grep
kill -9 <PID>  # Kill specific process by PID

# Start the server with reactive updates (remove watchdog for polling mode)
nohup uv run marimo edit front_modeling.py --watch --port 8080 > marimo_server.log 2>&1 &
```

### 4. Access Your Notebook
- Check server logs: `tail -f marimo_server.log`
- Get URL from logs (includes access token)
- Example: `http://localhost:8080?access_token=VwBxksTubFRhGAQeVrnRwg`

### 5. Verify Reactive Updates
- Make changes to your `.py` file
- Watch the browser automatically update with new cell outputs
- Changes trigger auto-execution when `watcher_on_save = "autorun"`

## Programmatic Output Inspection Workflow

### Export and Parse Computed Outputs
```bash
# 1. Make changes with Edit tool or external editor
# 2. Export static HTML (much easier to parse than live server)
uv run marimo export html front_modeling.py -o snapshot.html

# 3. Extract computed values
grep "Total facilities: [0-9]*" snapshot.html
python3 parse_marimo_export.py  # Custom parser for session data
```

### Why Static Export vs Live Server:
- **Static export**: Clean JSON session data, easy parsing, 43KB
- **Live server curl**: Empty shell + JavaScript, no computed outputs
- **Playwright conversion**: Fully rendered DOM, 235KB, harder to parse

### Session Structure Navigation:
- **`notebook.cells[]`**: Code definitions with `id`, `code`, `code_hash`
- **`session.cells[]`**: Computed outputs with `id`, `outputs[]`, `console[]`
- **Cell IDs**: Use for mapping between code and outputs (e.g., `vblA`, `bkHC`)
- **Named cells**: Define functions like `def current_data_overview(mo):` for better navigation

## Server Management Commands

**Check running processes:**
```bash
ps aux | grep -E "marimo|python.*marimo" | grep -v grep
```

**Stop server properly:**
```bash
kill -9 <PID>  # Use specific PID from ps output
```

**Check server logs:**
```bash
tail -f marimo_server.log
tail -f ~/.cache/marimo/logs/marimo.log  # System logs
```

**View configuration:**
```bash
uv run marimo config show
```

## Troubleshooting

### Reactive Updates Not Working:
1. **Check global config**: `~/.config/marimo/marimo.toml` must have `watcher_on_save = "autorun"`
2. **Remove watchdog**: `uv remove watchdog` to force polling mode (more reliable with Edit tool)
3. **Manual save**: Hit Cmd-S in external editor to trigger file watcher

### Export Watch Mode Bug:
- `marimo export html --watch` has asyncio bug in v0.14.10
- Use manual export instead: `uv run marimo export html notebook.py -o output.html`

### File Watcher Issues:
- **Watchdog mode**: Detects editor saves (Cmd-S) but not programmatic Edit tool changes
- **Polling mode**: Detects all file modification time changes (works with Edit tool)

## Core Commands

**Core Commands:**
- `marimo edit` - Create or edit notebooks (opens editor)
- `marimo edit notebook.py` - Create/edit a specific notebook
- `marimo run notebook.py` - Run notebook as read-only app
- `marimo new` - Create empty notebook or generate from prompt

**Utility Commands:**
- `marimo tutorial` - Open tutorials (`marimo tutorial intro`)
- `marimo convert` - Convert Jupyter/Markdown to marimo
- `marimo export` - Export to various formats
- `marimo config` - Configure marimo settings
- `marimo env` - Print environment info for debugging

**Common workflow:**
1. `uv run marimo edit` - Start editing mode
2. `uv run marimo run your_notebook.py` - Run as app
3. `uv run marimo tutorial intro` - Learn basics

**For your project:**
- `uv run marimo edit gov_data.py` - Edit your government data notebook
- `uv run marimo run gov_data.py` - Run it as an app

The notebooks are Python files with `@app.cell` decorators, so they can be version controlled and edited in any IDE.