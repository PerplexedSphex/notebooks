# Marimo Reactive Notebook Development Loop

## ⚡ IMPORTANT: "just" Command Reference
When user says "just run it" or "just <command>", they are referring to the `just` command runner (justfile) in this directory. The `just` command is the primary workflow tool for notebook operations.

## 🚨 FIRST STEP FOR ALL AGENTS: Establish Your Identity

**Before doing ANYTHING else, agents must establish their identity:**

```bash
# For system agent:
AGENT_NAME=system just set notebook.py

# For T-Mobile agent:  
AGENT_NAME=tmobile just set notebook.py

# For Ryder agent:
AGENT_NAME=ryder just set notebook.py
```

**ALL subsequent commands must use your AGENT_NAME:**
```bash
AGENT_NAME=system just current
AGENT_NAME=system just export  
AGENT_NAME=system just verify
```

This prevents agents from interfering with each other's notebook settings.

## Quick Reference Commands

```bash
# Agent-Namespaced Commands (ALWAYS use AGENT_NAME=yourname):
AGENT_NAME=system just set notebook.py        # Set current notebook for your agent
AGENT_NAME=system just current                # Show your agent's current notebook
AGENT_NAME=system just export                 # Export your agent's notebook
AGENT_NAME=system just verify                 # AI-powered analysis (fast)
AGENT_NAME=system just verify-pro             # Detailed analysis (thorough)
AGENT_NAME=system just kill                   # Kill your agent's current notebook
AGENT_NAME=system just loop                   # Full cycle: current → status → export → verify

# Shared Commands (affect all agents):
just status                                   # Show all running marimo servers
just kill-notebook notebook.py               # Kill specific notebook processes  
just kill-all                                # Kill ALL marimo processes
just run notebook.py                         # Start notebook on auto-port (session-specific)
just help                                    # Show all available commands
```

## Development Workflow

### Agent Setup (REQUIRED FIRST STEP):
1. **Establish Identity**: `AGENT_NAME=system just set notebook.py` 
2. **Verify Setup**: `AGENT_NAME=system just current`

### Agent Workflow (Isolated):
1. **Set Notebook**: `AGENT_NAME=system just set notebook.py` (creates `.current_notebook_system`)
2. **Development Loop**: `AGENT_NAME=system just loop` (current → status → export → verify) 
3. **Quick Iteration**: Edit files → `AGENT_NAME=system just export` → `AGENT_NAME=system just verify`
4. **Cleanup**: `AGENT_NAME=system just kill` (kills only your agent's notebook)

### Multi-Agent Safety:
- Each agent has isolated `.current_notebook_AGENTNAME` file
- `AGENT_NAME=system` vs `AGENT_NAME=tmobile` vs `AGENT_NAME=ryder` are completely separate
- Shared commands like `just status` and `just run notebook.py` still work for coordination

### AI-Powered Verification:
- **`just verify`**: Fast feedback using Gemini Flash - perfect for quick iteration
- **`just verify-pro`**: Comprehensive analysis using Gemini Pro - ideal for thorough reviews
- Both analyze: data quality, visualization effectiveness, code issues, and improvement suggestions

### Export Error Handling:
- Export failures with execution errors still create HTML files for analysis
- **Future Enhancement**: Pipe export errors to triage system similar to success exports
- Use `just logs` to review server execution details when exports fail

### Smart Context Management:
- Notebook setting persists across command invocations
- All commands automatically use the configured notebook
- No need to repeatedly specify notebook names

## Key Insights

- **Reactive Updates**: Changes to data automatically recompute dependent cells
- **Edit Tool Works**: File modifications via Edit tool trigger marimo's file watcher
- **HTML Export**: Static exports contain computed session data for easy parsing
- **Server URL**: Format is `http://localhost:8080?access_token=<token>`

## Debugging & Error Handling

### Quick Debug Commands
```bash
just logs                          # Check recent server logs
just editor-logs                   # Check editor server logs  
just status                        # See all running servers with PIDs/ports
```

### Log Locations
- **Server logs**: `logs/marimo_server.log` (general server)
- **Notebook logs**: `logs/marimo_notebook_port.log` (specific instances)
- **Global logs**: `~/.cache/marimo/logs/marimo.log` (system-wide marimo)
- **Editor logs**: `logs/marimo_editor.log` (editor server)

### Debugging Methods
1. **Console Output**: Print statements and errors appear below cells in browser
2. **Interactive Debugging**: `breakpoint()` works normally for PDB debugging
3. **Visual Tools**: Variable panel (sidebar) shows reactive dependencies
4. **Error Capture**: HTML exports include console output and error messages
5. **Browser DevTools**: Full access to browser debugging for frontend issues

### Common Issues & Solutions
- **File watcher warnings**: Ignore "watchdog not installed" warnings (intentionally not using watchdog)  
- **Server conflicts**: Use `just status` to see port usage, `just kill-notebook` for targeted cleanup
- **Mass cleanup**: Use `just kill-all` to terminate all marimo processes
- **Export failures**: Check `just logs` for execution errors, HTML still created
- **Token issues**: Restart server to resolve access token problems

### Agent REPL Workflow Performance
- **Iteration cycle**: Edit → `just export` → `just verify` (~15 seconds)
- **Export reliability**: Works even with execution errors
- **AI feedback quality**: Comprehensive analysis of data, code, and visualizations
- **Debugging visibility**: Complete session state captured in HTML exports

## Project Context

Environmental regulatory data analysis with EPA datasets (RCRA, ECHO, ICIS-AIR, NPDES). DuckDB storage, Plotly visualizations, reactive dashboards.