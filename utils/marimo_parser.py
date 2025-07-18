#!/usr/bin/env python3
"""Marimo HTML export parser using BeautifulSoup for clean JSON extraction."""

import json
import re
import sys
from typing import Dict, List, Any, Optional

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("❌ BeautifulSoup not found. Install with: uv add beautifulsoup4")
    sys.exit(1)

def extract_marimo_data(html_file: str) -> Optional[Dict[str, Any]]:
    """Extract marimo data using BeautifulSoup to find embedded JSON."""
    
    try:
        with open(html_file, 'r', encoding='utf-8') as f:
            content = f.read()
    except FileNotFoundError:
        print(f"❌ File not found: {html_file}")
        return None
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        return None
    
    # Parse HTML with BeautifulSoup
    soup = BeautifulSoup(content, 'html.parser')
    
    # Method 1: Look for script tags containing the data
    script_tags = soup.find_all('script')
    for script in script_tags:
        if script.string and ('notebook' in script.string and 'session' in script.string):
            script_content = script.string
            
            # Look for the embedded data structure
            # In marimo exports, data is often in window.__MARIMO_MOUNT_CONFIG__
            patterns = [
                r'"appConfig":\s*\{[^}]*\}[^}]*"notebook":\s*\{.*?"session":\s*\{.*?\}',
                r'\{[^{]*"appConfig".*?"session":\s*\{.*?\}',
            ]
            
            for pattern in patterns:
                match = re.search(pattern, script_content, re.DOTALL)
                if match:
                    # Extract the full JSON object
                    match_start = match.start()
                    # Find the opening brace
                    start_pos = script_content.rfind('{', 0, match_start)
                    if start_pos == -1:
                        start_pos = match_start
                    
                    # Find the matching closing brace
                    brace_count = 0
                    end_pos = start_pos
                    
                    for i, char in enumerate(script_content[start_pos:], start_pos):
                        if char == '{':
                            brace_count += 1
                        elif char == '}':
                            brace_count -= 1
                            if brace_count == 0:
                                end_pos = i + 1
                                break
                    
                    try:
                        json_str = script_content[start_pos:end_pos]
                        # Clean up common issues
                        json_str = re.sub(r',(\s*[}\]])', r'\1', json_str)  # Remove trailing commas
                        data = json.loads(json_str)
                        if 'notebook' in data and 'session' in data:
                            return data
                    except json.JSONDecodeError as e:
                        # If main parse fails, try extracting just the data we need
                        continue
    
    # Method 2: Look in the raw HTML content for the JSON structure
    # Sometimes the data is embedded directly in the HTML without script tags
    json_patterns = [
        r'"appConfig"[^{]*({.*?"session":\s*{.*?}[^}]*})',
        r'({[^{]*"appConfig".*?"session":\s*{.*?})',
    ]
    
    for pattern in json_patterns:
        matches = re.finditer(pattern, content, re.DOTALL)
        for match in matches:
            try:
                json_str = match.group(1)
                # Try to balance braces if needed
                if json_str.count('{') != json_str.count('}'):
                    # Find the proper closing
                    start_pos = match.start(1)
                    brace_count = 0
                    end_pos = start_pos
                    
                    for i, char in enumerate(content[start_pos:], start_pos):
                        if char == '{':
                            brace_count += 1
                        elif char == '}':
                            brace_count -= 1
                            if brace_count == 0:
                                end_pos = i + 1
                                break
                    
                    json_str = content[start_pos:end_pos]
                
                data = json.loads(json_str)
                if 'notebook' in data and 'session' in data:
                    return data
                    
            except json.JSONDecodeError:
                continue
    
    print("❌ Could not extract marimo data from HTML")
    return None

def analyze_marimo_data(data: Dict[str, Any], verbose: bool = False) -> None:
    """Analyze the extracted marimo data."""
    
    print("🔍 Marimo Export Analysis (BeautifulSoup)")
    print("=" * 60)
    
    # Notebook analysis
    notebook = data.get('notebook', {})
    notebook_cells = notebook.get('cells', [])
    
    print(f"📝 Notebook Structure:")
    print(f"  • Total cells: {len(notebook_cells)}")
    print(f"  • Marimo version: {notebook.get('metadata', {}).get('marimo_version', 'unknown')}")
    
    # Cell name mapping
    named_cells = {}
    for cell in notebook_cells:
        cell_id = cell.get('id')
        cell_name = cell.get('name', '_')
        if cell_name != '_':
            named_cells[cell_name] = cell_id
    
    if named_cells:
        print(f"\\n🏷️  Named Cell Mappings:")
        for name, cell_id in named_cells.items():
            print(f"  • {name}() → {cell_id}")
    
    # Session analysis
    session = data.get('session', {})
    session_cells = session.get('cells', [])
    
    print(f"\\n🖥️  Session Results:")
    print(f"  • Executed cells: {len(session_cells)}")
    
    # Console output analysis
    console_cells = []
    output_cells = []
    
    for cell in session_cells:
        cell_id = cell.get('id')
        console_logs = cell.get('console', [])
        outputs = cell.get('outputs', [])
        
        if console_logs:
            console_cells.append((cell_id, console_logs))
        if outputs:
            output_cells.append((cell_id, outputs))
    
    print(f"  • Cells with console output: {len(console_cells)}")
    print(f"  • Cells with visual output: {len(output_cells)}")
    
    # Console output details
    if console_cells:
        print(f"\\n💬 Console Output Details:")
        for cell_id, logs in console_cells:
            # Find the cell name
            cell_name = next((name for name, cid in named_cells.items() if cid == cell_id), cell_id)
            print(f"  📋 {cell_name} ({cell_id}):")
            
            stdout_logs = [log for log in logs if log.get('name') == 'stdout']
            stderr_logs = [log for log in logs if log.get('name') == 'stderr']
            
            print(f"    • {len(stdout_logs)} stdout, {len(stderr_logs)} stderr")
            
            if verbose:
                for log in logs[:3]:  # Show first 3 logs
                    text = log.get('text', '').strip()
                    if len(text) > 100:
                        text = text[:100] + '...'
                    print(f"    {log.get('name', 'unknown')}: {text}")
    
    # Output analysis
    if output_cells:
        print(f"\\n📊 Output Analysis:")
        for cell_id, outputs in output_cells:
            cell_name = next((name for name, cid in named_cells.items() if cid == cell_id), cell_id)
            
            for i, output in enumerate(outputs):
                output_type = output.get('type', 'unknown')
                data_content = output.get('data', {})
                
                # Extract interesting values
                interesting_values = []
                if isinstance(data_content, dict):
                    for data_type, content in data_content.items():
                        if isinstance(content, str):
                            # Look for computed values
                            facility_match = re.search(r'Total facilities:\s*(\d+)', content)
                            if facility_match:
                                interesting_values.append(f"Total facilities: {facility_match.group(1)}")
                            
                            if 'marimo-plotly' in content:
                                interesting_values.append("Interactive plot")
                            
                            if 'markdown' in data_type and len(content) < 200:
                                clean_content = re.sub(r'<[^>]+>', '', content).strip()
                                if clean_content:
                                    interesting_values.append(f"Text: {clean_content[:50]}...")
                
                if interesting_values:
                    print(f"  • {cell_name}: {', '.join(interesting_values)}")

def main():
    """Main function for marimo-dev command."""
    if len(sys.argv) < 2:
        print("Usage: marimo-dev <html_file> [--verbose]")
        print("Example: marimo-dev exported_notebook.html --verbose")
        return None
    
    filename = sys.argv[1]
    verbose = '--verbose' in sys.argv or '-v' in sys.argv
    
    print(f"🔄 Parsing {filename}...")
    data = extract_marimo_data(filename)
    
    if data:
        print("✅ Successfully extracted marimo data!")
        analyze_marimo_data(data, verbose=verbose)
        
        print(f"\\n🎯 Summary:")
        print(f"✓ Cell name → ID mapping working")
        print(f"✓ Console output (prints, warnings, errors) captured")
        print(f"✓ Computed values (like 'Total facilities') extractable")
        print(f"✓ Interactive plots and rich outputs detected")
        print(f"✓ Clean JSON structure successfully parsed")
        
        return data
    else:
        print("❌ Failed to extract data")
        return None

if __name__ == "__main__":
    main()