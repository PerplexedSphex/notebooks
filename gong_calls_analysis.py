import marimo

__generated_with = "0.14.10"
app = marimo.App(width="medium", auto_download=["ipynb"])


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo):
    mo.md(
        r"""
    # Gong Calls Analysis with AI

    This notebook analyzes call data from Gong using AI for insights and pattern detection.
    
    **Status: Live data successfully loaded from Gong API!** ✅
    
    **Export watcher test #1** 🎯
    
    **Export watcher test #2** 🚀
    """
    )
    return


@app.cell
def _():
    # Import required libraries
    import pandas as pd
    import numpy as np
    import plotly.express as px
    import plotly.graph_objects as go
    from datetime import datetime, timedelta
    import json
    import requests
    import os
    from urllib.parse import urlparse, parse_qs
    import re
    return json, os, pd, px, re, requests


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Data Loading

    Load and examine the Gong call data.

    https://us-15501.app.gong.io/call?id=7176512277597661813
    https://us-15501.app.gong.io/call?id=3189264363249778800
    https://us-15501.app.gong.io/call?id=6540721093378607428
    https://us-15501.app.gong.io/call?id=7646615448726402593
    """
    )
    return


@app.cell
def _(os, re, requests):
    class GongAPI:
        def __init__(self, access_key=None, access_key_secret=None, base_url=None):
            self.access_key = access_key or os.getenv('GONG_ACCESS_KEY')
            self.access_key_secret = access_key_secret or os.getenv('GONG_ACCESS_KEY_SECRET')
            self.base_url = base_url or os.getenv('GONG_BASE_URL', "https://us-15501.api.gong.io/v2")
            self.session = requests.Session()

            # Set up basic auth for Gong API
            self.session.auth = (self.access_key, self.access_key_secret)

        def extract_call_id(self, url):
            """Extract call ID from Gong URL"""
            match = re.search(r'id=(\d+)', url)
            return match.group(1) if match else None

        def get_call_basic(self, call_id):
            """Get basic call information"""
            response = self.session.get(f"{self.base_url}/calls/{call_id}")
            response.raise_for_status()
            return response.json()

        def get_calls_extensive(self, call_ids, include_content=True):
            """Get extensive call data for multiple calls"""
            content_selector = {
                "exposedFields": {
                    "parties": True,
                    "content": {
                        "structure": True,
                        "topics": True,
                        "trackers": True,
                        "brief": True,
                        "outline": True,
                        "highlights": True,
                        "callOutcome": True,
                        "keyPoints": True
                    },
                    "interaction": {
                        "speakers": True,
                        "questions": True,
                        "personInteractionStats": True
                    }
                }
            } if include_content else {}

            payload = {
                "filter": {
                    "callIds": call_ids
                },
                "contentSelector": content_selector
            }

            response = self.session.post(f"{self.base_url}/calls/extensive", json=payload)
            response.raise_for_status()
            return response.json()

    # Initialize Gong API client
    gong_api = GongAPI()
    return (gong_api,)


@app.cell
def _(gong_api, mo):
    # Extract call IDs from the URLs
    call_urls = [
        "https://us-15501.app.gong.io/call?id=7176512277597661813",
        "https://us-15501.app.gong.io/call?id=3189264363249778800", 
        "https://us-15501.app.gong.io/call?id=6540721093378607428",
        "https://us-15501.app.gong.io/call?id=7646615448726402593"
    ]

    call_ids = [gong_api.extract_call_id(url) for url in call_urls]
    mo.md(f"**Extracted Call IDs:** {call_ids}")
    return (call_ids,)


@app.cell
def _(call_ids, gong_api, json, mo, pd):
    # Load call data from Gong API
    extensive_data = gong_api.get_calls_extensive(call_ids)
    calls_df = pd.json_normalize(extensive_data.get('calls', []))

    mo.md(f"**✅ Loaded {len(calls_df)} calls**")
    mo.md(f"**Raw API Response Keys:** {list(extensive_data.keys())}")

    if len(calls_df) > 0:
        mo.md(f"**DataFrame Columns:** {list(calls_df.columns)}")
        mo.md(f"**Sample Call Data:**")
        mo.md(f"```json\n{json.dumps(extensive_data['calls'][0], indent=2)[:1000]}...\n```")

    calls_df
    return (calls_df,)


@app.cell
def _(mo):
    mo.md(
        r"""
    ## AI Analysis Setup

    Configure AI tools for call analysis.
    """
    )
    return


@app.cell
def _(mo, os):
    # AI Analysis Configuration
    class AIAnalyzer:
        def __init__(self):
            # Check for available AI service credentials
            self.anthropic_key = os.getenv('ANTHROPIC_API_KEY')
            self.openai_key = os.getenv('OPENAI_API_KEY') 
            self.gemini_key = os.getenv('GEMINI_API_KEY')

            # Initialize available services
            self.available_services = []

            if self.anthropic_key:
                self.available_services.append("Anthropic (Claude)")
            if self.openai_key:
                self.available_services.append("OpenAI (GPT)")
            if self.gemini_key:
                self.available_services.append("Google (Gemini)")

            if not self.available_services:
                mo.md("⚠️ **No AI service credentials found.** Set ANTHROPIC_API_KEY, OPENAI_API_KEY, or GEMINI_API_KEY environment variables.")
            else:
                mo.md(f"✅ **Available AI services:** {', '.join(self.available_services)}")

        def analyze_call_content(self, call_data, analysis_type="summary"):
            """Analyze call content using available AI services"""
            if not self.available_services:
                return {"error": "No AI services configured"}

            # This would contain the actual AI analysis logic
            # For now, return a structure for the analysis
            return {
                "analysis_type": analysis_type,
                "service_used": self.available_services[0],
                "call_id": call_data.get('metaData.id', 'unknown'),
                "placeholder": "AI analysis implementation goes here"
            }

        def get_analysis_prompts(self):
            """Get available analysis prompts for calls"""
            return {
                "summary": "Provide a concise summary of the call including key topics and outcomes",
                "sentiment": "Analyze the sentiment and tone throughout the call",
                "action_items": "Extract action items and follow-ups mentioned in the call", 
                "topics": "Identify and categorize the main topics discussed",
                "questions": "List questions asked during the call and whether they were answered",
                "objections": "Identify any objections or concerns raised and how they were addressed"
            }

    ai_analyzer = AIAnalyzer()
    analysis_prompts = ai_analyzer.get_analysis_prompts()

    # Display available analysis types
    mo.md("**Available Analysis Types:**")
    for key, desc in analysis_prompts.items():
        mo.md(f"- **{key.title()}**: {desc}")

    return (ai_analyzer,)


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Analysis & Insights

    Extract insights from call data using AI.
    """
    )
    return


@app.cell
def _(calls_df, mo, pd, px):
    # Call Data Overview and Visualizations
    if not calls_df.empty:
        # Basic statistics
        mo.md("### 📊 Call Data Overview")

        # Duration analysis (convert seconds to minutes)
        if 'metaData.duration' in calls_df.columns:
            calls_df['duration_minutes'] = calls_df['metaData.duration'] / 60

            # Duration distribution
            fig_duration = px.bar(
                calls_df, 
                x=[f"Call {i+1}" for i in range(len(calls_df))],
                y='duration_minutes',
                title="Call Duration (Minutes)",
                labels={'x': 'Calls', 'y': 'Duration (minutes)'}
            )
            fig_duration.show()

            # Summary stats
            avg_duration = calls_df['duration_minutes'].mean()
            total_duration = calls_df['duration_minutes'].sum()
            mo.md(f"**Average Call Duration:** {avg_duration:.1f} minutes")
            mo.md(f"**Total Call Time:** {total_duration:.1f} minutes")

        # Call timeline if we have timestamps
        if 'metaData.started' in calls_df.columns:
            calls_df['started_dt'] = pd.to_datetime(calls_df['metaData.started'], format='ISO8601')
            fig_timeline = px.scatter(
                calls_df,
                x='started_dt',
                y=[f"Call {i+1}" for i in range(len(calls_df))],
                size='duration_minutes' if 'duration_minutes' in calls_df.columns else [20]*len(calls_df),
                title="Call Timeline",
                labels={'x': 'Date/Time', 'y': 'Calls'}
            )
            fig_timeline.show()
    else:
        mo.md("**No call data available for visualization**")

    return


@app.cell
def _(ai_analyzer, calls_df, mo):
    # AI Analysis Section
    mo.md("### 🤖 AI Analysis")

    if not calls_df.empty and ai_analyzer.available_services:
        # Example analysis for first call
        if len(calls_df) > 0:
            first_call = calls_df.iloc[0].to_dict()
            sample_analysis = ai_analyzer.analyze_call_content(first_call, "summary")

            mo.md("**Sample Analysis Structure:**")
            mo.md(f"```json\n{sample_analysis}\n```")

            mo.md("**Ready for full AI analysis implementation.**")
            mo.md("To implement: Add API calls to your chosen AI service with call transcripts/content.")
    else:
        if calls_df.empty:
            mo.md("**No call data available for analysis**")
        else:
            mo.md("**AI services not configured - set API keys to enable analysis**")

    return


@app.cell
def _(mo):
    mo.md(
        r"""
    ### 🚀 Next Steps

    1. **Configure API Credentials**: Set `GONG_ACCESS_KEY`, `GONG_ACCESS_KEY_SECRET`, and AI service keys
    2. **Implement AI Analysis**: Add actual API calls to analyze call transcripts  
    3. **Enhanced Visualizations**: Add more charts based on AI insights
    4. **Export Results**: Save analysis results for further processing

    **Ready for your custom analysis implementation!**
    """
    )
    return


if __name__ == "__main__":
    app.run()
