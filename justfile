# Marimo development workflow commands

# Set current notebook for all operations
set notebook:
    #!/usr/bin/env bash
    echo "{{notebook}}" > .current_notebook
    echo "📝 Set current notebook to: {{notebook}}"

# Get current notebook or exit with error
@_get_notebook:
    #!/usr/bin/env bash
    if [ -f .current_notebook ]; then
        cat .current_notebook
    else
        echo "❌ No notebook set. Use: just set <notebook.py>" >&2
        exit 1
    fi 2>/dev/null

# Show current notebook
current:
    #!/usr/bin/env bash
    if [ -f .current_notebook ]; then
        notebook=$(cat .current_notebook)
        echo "📓 Current notebook: $notebook"
    else
        echo "❌ No notebook set. Use: just set <notebook.py>"
        exit 1
    fi

# Kill existing marimo processes
kill:
    pkill -f marimo || echo "No marimo processes found"

# Start marimo server with file watching
start port="8080":
    #!/usr/bin/env bash
    notebook=$(just _get_notebook)
    mkdir -p logs
    echo "🚀 Starting marimo server for $notebook on port {{port}}"
    echo "📝 Logs: logs/marimo_server.log"
    nohup uv run marimo edit "$notebook" --watch --port {{port}} > logs/marimo_server.log 2>&1 &
    echo "✅ Server started, check logs: just logs"

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

# Full development cycle: kill → start → export → verify
loop:
    #!/usr/bin/env bash
    notebook=$(just _get_notebook)
    echo "🔄 Running full marimo development loop for $notebook"
    echo "=" | tr '=' '=' | head -c 50; echo
    just kill
    just start
    sleep 3
    just export
    just logs

# Show available commands
help:
    @just --list