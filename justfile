# Marimo development workflow commands

# Available notebooks for autocomplete (auto-discovered .py files)
_notebooks := `ls *.py 2>/dev/null | tr '\n' ' ' || echo ""`

# Default command - show help and usage
default:
    @echo "🚀 Marimo Notebook Workflow"
    @echo ""
    @echo "Usage:"
    @echo "  just launch <notebook.py>  - Start both edit and export for notebook"
    @echo "  just run <notebook.py>     - Start edit server only"  
    @echo "  just export <notebook.py>  - Start export watcher only"
    @echo "  just kill <notebook.py>    - Kill processes for notebook"
    @echo "  just status                - Show running processes"
    @echo "  just kill-all              - Kill all marimo processes"
    @echo ""
    @echo "Available notebooks:"
    @ls *.py 2>/dev/null | sed 's/^/  /' || echo "  No .py files found"

# Launch both edit and export for a notebook
launch notebook:
    @echo "🚀 Launching {{notebook}} with both edit and export..."
    @just run {{notebook}}
    @just export {{notebook}}

# Kill all marimo processes
kill-all:
    #!/usr/bin/env bash
    echo "🔍 Killing all marimo processes..."
    if pkill -f marimo; then
        echo "✅ All marimo processes terminated"
    else
        echo "❌ No marimo processes found"
    fi

# Kill specific notebook processes (both edit and export)
kill notebook:
    #!/usr/bin/env bash
    echo "🔍 Looking for {{notebook}} processes..."
    # Kill both edit and export processes for this notebook
    edit_pids=$(ps aux | grep "[m]arimo edit {{notebook}}" | awk '{print $2}')
    export_pids=$(ps aux | grep "[m]arimo export ipynb {{notebook}}" | awk '{print $2}')
    
    killed=false
    if [ -n "$edit_pids" ]; then
        echo "💀 Killing {{notebook}} edit processes: $edit_pids"
        echo "$edit_pids" | xargs kill
        killed=true
    fi
    
    if [ -n "$export_pids" ]; then
        echo "💀 Killing {{notebook}} export processes: $export_pids"  
        echo "$export_pids" | xargs kill
        killed=true
    fi
    
    if [ "$killed" = true ]; then
        echo "✅ {{notebook}} processes terminated"
    else
        echo "❌ No {{notebook}} processes found"
    fi

# Start notebook on next available port with watch mode
run notebook:
    #!/usr/bin/env bash
    port=8080
    while lsof -i :$port >/dev/null 2>&1; do
        ((port++))
    done
    mkdir -p logs
    logfile="logs/marimo_{{notebook}}_${port}.log"
    echo "🚀 Starting {{notebook}} on port $port"
    echo "📝 Logs: $logfile"
    nohup uv run marimo edit "{{notebook}}" --watch --port $port > "$logfile" 2>&1 &
    echo "✅ http://localhost:$port"

# Export notebook to IPYNB with watch mode (auto-export on changes)
export notebook:
    #!/usr/bin/env bash
    notebook_base=$(basename "{{notebook}}" .py)
    output_file="${notebook_base}.ipynb"
    mkdir -p logs
    logfile="logs/marimo_export_${notebook_base}.log"
    echo "👁️  Watching {{notebook}} and auto-exporting to ${output_file}"
    echo "📝 Logs: $logfile"
    nohup uv run marimo export ipynb "{{notebook}}" -o "${output_file}" --watch > "$logfile" 2>&1 &
    echo "✅ Export watcher started in background"

# Show running marimo processes (both edit and export)
status:
    #!/usr/bin/env bash
    echo "🔍 Running marimo processes:"
    echo
    echo "📊 Edit servers:"
    ps aux | grep "[m]arimo edit" | while read line; do
        pid=$(echo "$line" | awk '{print $2}')
        port=$(echo "$line" | sed -n 's/.*--port \([0-9]*\).*/\1/p')
        notebook=$(echo "$line" | sed -n 's/.*marimo edit \([^ ]*\).*/\1/p')
        if [ -n "$port" ] && [ -n "$notebook" ]; then
            echo "  📝 $notebook on port $port (PID: $pid) - http://localhost:$port"
        fi
    done
    echo
    echo "👁️  Export watchers:"
    ps aux | grep "[m]arimo export" | while read line; do
        pid=$(echo "$line" | awk '{print $2}')
        # Extract notebook name from the command line
        notebook=$(echo "$line" | sed -n 's/.*marimo export ipynb \([^[:space:]]*\) -o.*/\1/p')
        if [ -n "$notebook" ]; then
            echo "  📤 $notebook (PID: $pid)"
        else
            echo "  📤 unknown notebook (PID: $pid)"
        fi
    done

# Sync dependencies and install missing packages
sync:
    #!/usr/bin/env bash
    echo "🔄 Syncing uv dependencies..."
    uv sync
    echo "✅ Dependencies synced"

# Show available commands
help:
    @just --list