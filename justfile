# Marimo development workflow commands

# Available notebooks for autocomplete
_notebooks := "main.py gov_data.py account_rcra.py industry_benchmarking.py tmobile_campaign.py ryder_campaign.py front_modeling.py"

# Agent-namespaced notebook management
# Usage: AGENT_NAME=system just set notebook.py
# Usage: AGENT_NAME=system just current

# Set current notebook for agent
set notebook:
    #!/usr/bin/env bash
    if [ -z "$AGENT_NAME" ]; then
        echo "❌ AGENT_NAME not set. Usage: AGENT_NAME=system just set notebook.py"
        exit 1
    fi
    echo "{{notebook}}" > ".current_notebook_$AGENT_NAME"
    echo "📝 Set current notebook for $AGENT_NAME: {{notebook}}"

# Get current notebook for agent or exit with error
_get_notebook:
    #!/usr/bin/env bash
    if [ -z "$AGENT_NAME" ]; then
        echo "❌ AGENT_NAME not set. Usage: AGENT_NAME=system just current" >&2
        exit 1
    fi
    if [ -f ".current_notebook_$AGENT_NAME" ]; then
        cat ".current_notebook_$AGENT_NAME"
    else
        echo "❌ No notebook set for $AGENT_NAME. Run: AGENT_NAME=$AGENT_NAME just set <notebook.py>" >&2
        exit 1
    fi

# Show current notebook for agent
current:
    #!/usr/bin/env bash
    if [ -z "$AGENT_NAME" ]; then
        echo "❌ AGENT_NAME not set. Usage: AGENT_NAME=system just current"
        exit 1
    fi
    if [ -f ".current_notebook_$AGENT_NAME" ]; then
        notebook=$(cat ".current_notebook_$AGENT_NAME")
        echo "📓 Current notebook for $AGENT_NAME: $notebook"
    else
        echo "❌ No notebook set for $AGENT_NAME."
        echo "🔄 Run: AGENT_NAME=$AGENT_NAME just set <notebook.py>"
        exit 1
    fi

# Kill all marimo processes
kill-all:
    pkill -f marimo || echo "No marimo processes found"

# Kill specific notebook process
kill-notebook notebook:
    #!/usr/bin/env bash
    echo "🔍 Looking for {{notebook}} processes..."
    pids=$(ps aux | grep "[m]arimo edit {{notebook}}" | awk '{print $2}')
    if [ -n "$pids" ]; then
        echo "💀 Killing {{notebook}} processes: $pids"
        echo "$pids" | xargs kill
        echo "✅ {{notebook}} processes terminated"
    else
        echo "❌ No {{notebook}} processes found"
    fi

# Kill current notebook (from .current_notebook)
kill:
    #!/usr/bin/env bash
    notebook=$(just _get_notebook)
    just kill-notebook "$notebook"

# Show running marimo servers
status:
    #!/usr/bin/env bash
    echo "🔍 Checking running marimo servers..."
    ps aux | grep "[m]arimo edit" | while read line; do
        pid=$(echo "$line" | awk '{print $2}')
        port=$(echo "$line" | sed -n 's/.*--port \([0-9]*\).*/\1/p')
        notebook=$(echo "$line" | sed -n 's/.*marimo edit \([^ ]*\).*/\1/p')
        if [ -n "$port" ] && [ -n "$notebook" ]; then
            echo "📊 $notebook on port $port (PID: $pid) - http://localhost:$port"
        fi
    done

# Start editor server for notebook selection (background)
serve port="2718":
    #!/usr/bin/env bash
    mkdir -p logs
    echo "🚀 Starting marimo editor server on port {{port}}"
    echo "📝 Logs: logs/marimo_editor.log"
    nohup uv run marimo edit --port {{port}} > logs/marimo_editor.log 2>&1 &
    sleep 2
    echo "✅ Editor server started"
    echo "🌐 URL: http://localhost:{{port}}"
    echo "📋 Check logs: just editor-logs"

# Start marimo server with file watching
start port="8080":
    #!/usr/bin/env bash
    notebook=$(just _get_notebook)
    mkdir -p logs
    echo "🚀 Starting marimo server for $notebook on port {{port}}"
    echo "📝 Logs: logs/marimo_server.log"
    nohup uv run marimo edit "$notebook" --watch --port {{port}} > logs/marimo_server.log 2>&1 &
    echo "✅ Server started, check logs: just logs"

# Start any notebook on next available port
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

# Export notebook to HTML (no timestamp)
export:
    #!/usr/bin/env bash
    notebook=$(just _get_notebook)
    mkdir -p logs
    notebook_base=$(basename "$notebook" .py)
    output_file="logs/${notebook_base}.html"
    echo "📄 Exporting $notebook to ${output_file}"
    if uv run marimo export html "$notebook" -o "${output_file}"; then
        echo "✅ Exported to ${output_file}"
    else
        echo "❌ Export failed"
    fi

# Check recent server logs
logs:
    #!/usr/bin/env bash
    if [ -f logs/marimo_server.log ]; then
        echo "📋 Recent server logs:"
        tail -10 logs/marimo_server.log
    else
        echo "❌ Log file not found: logs/marimo_server.log"
    fi

# Check editor server logs
editor-logs:
    #!/usr/bin/env bash
    if [ -f logs/marimo_editor.log ]; then
        echo "📋 Editor server logs:"
        tail -10 logs/marimo_editor.log
    else
        echo "❌ Log file not found: logs/marimo_editor.log"
    fi

# Smart verification using Gemini AI
verify:
    #!/usr/bin/env bash
    notebook=$(just _get_notebook)
    notebook_base=$(basename "$notebook" .py)
    export_file="logs/${notebook_base}.html"
    
    if [ ! -f "$export_file" ]; then
        echo "❌ Export file not found: $export_file"
        echo "💡 Run 'just export' first"
        exit 1
    fi
    
    echo "🔍 AI-powered verification of $export_file..."
    cat "$export_file" | gemini -p "Analyze this marimo notebook export for quality and completeness. Focus on: Data Quality (errors, warnings, failed computations), Analysis Completeness (insights present/missing), Visualization Quality (chart effectiveness), Code Quality (logic/approach issues), and Next Steps (improvement suggestions). Be concise but thorough. Use emojis for readability." -m "gemini-2.5-flash"

# Detailed verification using Gemini Pro
verify-pro:
    #!/usr/bin/env bash
    notebook=$(just _get_notebook)
    notebook_base=$(basename "$notebook" .py)
    export_file="logs/${notebook_base}.html"
    
    if [ ! -f "$export_file" ]; then
        echo "❌ Export file not found: $export_file"
        echo "💡 Run 'just export' first"
        exit 1
    fi
    
    echo "🔍 Detailed AI verification using Gemini Pro..."
    cat "$export_file" | gemini -p "As an expert data scientist and code reviewer, provide a comprehensive analysis of this marimo notebook. Evaluate: 1) Technical accuracy and data integrity, 2) Statistical validity of conclusions, 3) Code quality and best practices, 4) Visual design effectiveness, 5) Missing analytical depth, 6) Specific actionable recommendations for improvement. Include concrete suggestions for next development steps." -m "gemini-2.5-pro"

# Full development cycle: current → status → export → verify
loop:
    #!/usr/bin/env bash
    notebook=$(just _get_notebook)
    echo "🔄 Running development loop for $notebook"
    echo "=" | tr '=' '=' | head -c 50; echo
    just current
    just status
    just export
    just verify

# Sync dependencies and install missing packages
sync:
    #!/usr/bin/env bash
    echo "🔄 Syncing uv dependencies..."
    uv sync
    echo "✅ Dependencies synced"

# Show available commands
help:
    @just --list