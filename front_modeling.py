import marimo

__generated_with = "0.14.10"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo):
    mo.md(
        """
    # Environmental Data Analysis Dashboard

    This notebook provides analysis of EPA regulatory data including:
    - Air quality monitoring (ICIS-AIR)
    - Water discharge permits (NPDES)
    - Hazardous waste tracking (RCRA)
    - Facility compliance data
    """
    )
    return


@app.cell
def current_data_overview(mo):
    import pandas as pd
    import plotly.express as px
    import plotly.graph_objects as go

    # TEST: Add some prints and intentional errors
    print("🔍 Loading current data overview...")
    print(f"📊 Debug: Processing environmental data")
    
    # Sample data for demonstration - TESTING REACTIVE UPDATES v6 - CHANGED!
    sample_data = pd.DataFrame({
        'facility_type': ['Air Quality', 'Water', 'Hazardous Waste', 'Mixed'],
        'count': [150, 175, 85, 65],
        'violations': [30, 42, 25, 18]
    })
    
    # Test warning
    import warnings
    warnings.warn("⚠️ This is a test warning about data freshness")
    
    try:
        # Intentional error that we'll catch
        test_division = 1 / 0
    except ZeroDivisionError as e:
        print(f"❌ Caught expected error: {e}")
        print("✅ Error handling working correctly")

    mo.md(f"## Current Data Overview\n\nTotal facilities: {sample_data['count'].sum()}\n\nLast updated: 2:29 PM - TESTING CONSOLE OUTPUT!")
    return px, sample_data


@app.cell
def _(mo, px, sample_data):
    # Create a bar chart of facility types
    fig = px.bar(sample_data, x='facility_type', y='count', 
                 title='Facilities by Type',
                 color='violations',
                 color_continuous_scale='Reds')

    mo.ui.plotly(fig)
    return


@app.cell
def _(mo, px, sample_data):
    # Add a violations chart
    violations_fig = px.pie(sample_data, names='facility_type', values='violations',
                           title='Violations Distribution by Facility Type')

    mo.ui.plotly(violations_fig)
    return


@app.cell
def _(mo, sample_data):
    # Add a summary statistics table
    summary_stats = {
        'Total Facilities': sample_data['count'].sum(),
        'Total Violations': sample_data['violations'].sum(),
        'Average Violations per Facility': round(sample_data['violations'].sum() / sample_data['count'].sum(), 2),
        'Highest Risk Facility Type': sample_data.loc[sample_data['violations'].idxmax(), 'facility_type']
    }

    mo.md(f"""
    ## Summary Statistics

    - **Total Facilities**: {summary_stats['Total Facilities']}
    - **Total Violations**: {summary_stats['Total Violations']}
    - **Average Violations per Facility**: {summary_stats['Average Violations per Facility']}
    - **Highest Risk Facility Type**: {summary_stats['Highest Risk Facility Type']}
    """)
    return


if __name__ == "__main__":
    app.run()
