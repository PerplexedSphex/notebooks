import marimo

__generated_with = "0.14.10"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Setup: Core Libraries and Dependencies

    Loading essential libraries for the T-Mobile campaign analysis:
    - **marimo**: Reactive notebook interface
    - **duckdb**: High-performance SQL analytics engine for large datasets
    - **plotly**: Interactive visualizations for compliance dashboards
    - **pandas**: Data manipulation and analysis
    - **pathlib**: Cross-platform file path handling
    """
    )
    return


@app.cell
def _():
    import marimo as mo
    import duckdb
    import plotly.express as px
    import pandas as pd
    from pathlib import Path
    return Path, duckdb, mo, px


@app.cell
def _(mo):
    mo.md(
        r"""
    # T-Mobile Telecom Campaign Analysis

    **Campaign Focus**: T-Mobile site permit tracking and compliance analysis
    **Deadline**: End of next week (Jenny)
    **Approach**: Consolidated view necessity and missed items analysis
    """
    )
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Database Setup: Loading EPA Environmental Datasets

    Creating in-memory DuckDB database and loading key EPA datasets for telecom analysis:

    **Core Datasets:**
    - **FRS (Facility Registry Service)**: Master facility database for site identification
    - **ICIS-AIR**: Air quality permits, violations, and compliance tracking

    **Strategy**: Start with facility identification in FRS, then cross-reference with
    program-specific databases to build comprehensive compliance picture.
    """
    )
    return


@app.cell
def _(Path, duckdb):
    # Database connection to existing ECHO database
    ECHO_DB_PATH = Path("~/db/echo.duckdb").expanduser()
    conn = duckdb.connect(database=ECHO_DB_PATH, read_only=True)

    return (conn,)


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Site Discovery: T-Mobile Facility Identification

    **Objective**: Identify all T-Mobile facilities in the EPA databases using multiple name variations.

    **Search Strategy**:
    - Multiple name patterns: "T-MOBILE", "T MOBILE", "TMOBILE"
    - Case-insensitive matching to catch naming variations
    - Focus on FRS as master facility database

    **Key Fields**: Facility name, location, SIC/NAICS codes for business classification
    """
    )
    return


@app.cell
def _(conn, mo):
    # Initial T-Mobile site identification
    tmobile_query = """
    SELECT 
        f.primary_name as facility_name,
        f.location_address,
        f.city_name,
        f.state_code,
        f.postal_code as zip_code,
        f.registry_id
    FROM national_facility_file f
    WHERE 
        UPPER(f.primary_name) LIKE '%T-MOBILE%' 
        OR UPPER(f.primary_name) LIKE '%TMOBILE%'
    ORDER BY f.state_code, f.city_name
    """

    tmobile_sites = conn.execute(tmobile_query).df()

    mo.ui.table(
        tmobile_sites,
        label="T-Mobile Sites in FRS Database"
    )
    return (tmobile_sites,)


@app.cell
def _(mo):
    mo.md(
        r"""
        ## Comprehensive Site Discovery: T-Mobile Across All EPA Datasets
        
        **Objective**: Find T-Mobile facilities across ALL EPA databases in ECHO to get complete picture.
        
        **Dataset Coverage**:
        - **FRS (National Facility File)**: Master registry - 1,777 sites found above
        - **ICIS Water Facilities**: Clean Water Act regulated facilities  
        - **ICIS-AIR Facilities**: Clean Air Act regulated facilities
        - **RCRA Facilities**: Hazardous waste regulated facilities
        
        **Strategy**: UNION all facility tables to identify T-Mobile sites that may appear 
        in one program but not others, revealing compliance gaps and permit consolidation opportunities.
        """
    )
    return


@app.cell
def _(conn, mo):
    # Comprehensive T-Mobile facility search across all EPA databases
    all_facilities_query = """
    -- FRS (National Facility File) - Master registry
    SELECT 
        'FRS' as data_source,
        f.primary_name as facility_name,
        f.location_address,
        f.city_name as city,
        f.state_code as state,
        f.postal_code as zip_code,
        f.registry_id
    FROM national_facility_file f
    WHERE 
        UPPER(f.primary_name) LIKE '%T-MOBILE%' 
        OR UPPER(f.primary_name) LIKE '%TMOBILE%'
    
    UNION ALL
    
    -- ICIS Water Facilities - Clean Water Act
    SELECT 
        'ICIS_WATER' as data_source,
        w.facility_name,
        w.location_address,
        w.city,
        w.state_code as state,
        w.zip as zip_code,
        w.npdes_id as registry_id
    FROM icis_facilities w
    WHERE 
        UPPER(w.facility_name) LIKE '%T-MOBILE%' 
        OR UPPER(w.facility_name) LIKE '%TMOBILE%'
    
    UNION ALL
    
    -- ICIS-AIR Facilities - Clean Air Act 
    SELECT 
        'ICIS_AIR' as data_source,
        a.facility_name,
        a.street_address as location_address,
        a.city,
        a.state,
        a.zip_code,
        a.registry_id
    FROM icis_air_facilities a
    WHERE 
        UPPER(a.facility_name) LIKE '%T-MOBILE%' 
        OR UPPER(a.facility_name) LIKE '%TMOBILE%'
    
    UNION ALL
    
    -- RCRA Facilities - Hazardous Waste
    SELECT 
        'RCRA' as data_source,
        r.facility_name,
        NULL as location_address,  -- Not available in RCRA
        r.city_name as city,
        r.state_code as state,
        r.zip_code,
        r.id_number as registry_id
    FROM rcra_facilities r
    WHERE 
        UPPER(r.facility_name) LIKE '%T-MOBILE%' 
        OR UPPER(r.facility_name) LIKE '%TMOBILE%'
    
    ORDER BY state, city, facility_name
    """
    
    all_tmobile_facilities = conn.execute(all_facilities_query).df()
    
    # Summary statistics
    source_summary = all_tmobile_facilities['data_source'].value_counts()
    
    mo.md(f"""
    ## All T-Mobile EPA Facilities Summary
    
    **Total facilities across all EPA databases**: {len(all_tmobile_facilities)}
    
    **By Data Source**:
    - **FRS (Master Registry)**: {source_summary.get('FRS', 0)} facilities
    - **ICIS Water**: {source_summary.get('ICIS_WATER', 0)} facilities  
    - **ICIS-AIR**: {source_summary.get('ICIS_AIR', 0)} facilities
    - **RCRA Waste**: {source_summary.get('RCRA', 0)} facilities
    
    **Key Insight**: Different regulatory programs capture different T-Mobile facilities, 
    demonstrating the need for consolidated permit tracking across all EPA programs.
    """)
    
    return (all_tmobile_facilities,)


@app.cell
def _(all_tmobile_facilities, mo):
    # Display the comprehensive facility list
    mo.ui.table(
        all_tmobile_facilities,
        label="Complete T-Mobile Facilities Across All EPA Databases"
    )
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Geographic Focus: California Analysis Priority

    **Strategic Focus**: California sites prioritized for detailed analysis.

    **Rationale**:
    - CERS (California Environmental Reporting System) provides comprehensive state-level data
    - High regulatory complexity = higher value proposition for permit consolidation
    - Reference point for national expansion approach
    """
    )
    return


@app.cell
def _(mo, tmobile_sites):
    # California focus analysis
    ca_tmobile = tmobile_sites[tmobile_sites['state_code'] == 'CA']

    mo.md(f"""
    ## California T-Mobile Sites

    **Total T-Mobile sites found**: {len(tmobile_sites)}
    **California T-Mobile sites**: {len(ca_tmobile)}

    California focus aligns with CERS database availability for reference analysis.
    """)
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Compliance Analysis: Air Quality Permits & Violations

    **Objective**: Analyze air quality compliance across telecom industry for competitive benchmarking.

    **Approach**:
    - Broad telecom industry search terms (cell, tower, wireless, major carriers)
    - Join facilities with violation history to identify compliance patterns
    - Aggregate violation counts and dates for risk assessment

    **Value Proposition**: Identify missed compliance items and violation trends
    that demonstrate need for better permit tracking systems.
    """
    )
    return


@app.cell
def _(conn, mo):
    # Air permit compliance analysis for telecom facilities
    telecom_air_query = """
    SELECT 
        af.facility_name,
        af.state,
        af.city,
        af.air_operating_status_code as operating_status_code,
        COUNT(DISTINCT av.activity_id) as violation_count,
        MAX(av.hpv_dayzero_date) as latest_violation_date
    FROM icis_air_facilities af
    LEFT JOIN icis_air_violation_history av ON af.pgm_sys_id = av.pgm_sys_id
    WHERE 
        UPPER(af.facility_name) LIKE '%CELL%'
        OR UPPER(af.facility_name) LIKE '%TOWER%'
        OR UPPER(af.facility_name) LIKE '%WIRELESS%'
        OR UPPER(af.facility_name) LIKE '%TELECOM%'
        OR UPPER(af.facility_name) LIKE '%T-MOBILE%'
        OR UPPER(af.facility_name) LIKE '%AT&T%'
        OR UPPER(af.facility_name) LIKE '%VERIZON%'
    GROUP BY af.facility_name, af.state, af.city, af.air_operating_status_code
    ORDER BY violation_count DESC, af.state
    """

    telecom_air_compliance = conn.execute(telecom_air_query).df()

    mo.ui.table(
        telecom_air_compliance.head(20),
        label="Telecom Facility Air Compliance Overview"
    )
    return (telecom_air_compliance,)


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Geographic Distribution: Telecom Facility Density by State

    **Visualization Purpose**: Show geographic concentration of telecom air facilities
    to identify high-priority markets for T-Mobile campaign.

    **Strategic Insight**: States with higher facility counts represent:
    - More complex regulatory environments
    - Greater compliance tracking challenges
    - Higher potential value for permit consolidation services
    """
    )
    return


@app.cell
def _(mo, px, telecom_air_compliance):
    # Visualization: State distribution of telecom air facilities
    state_counts = telecom_air_compliance.groupby('state').size().reset_index(name='facility_count')

    fig = px.bar(
        state_counts.head(15), 
        x='state', 
        y='facility_count',
        title="Telecom Air Facilities by State (Top 15)",
        labels={'facility_count': 'Number of Facilities'}
    )

    mo.ui.plotly(fig)
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Next Analysis Steps

    1. **Site Identification**: Expand T-Mobile site discovery across all datasets (RCRA, Water, etc.)
    2. **Permit Consolidation**: Cross-reference permits across air, water, waste programs
    3. **Compliance Gaps**: Identify facilities with missing permits or violations
    4. **California Deep Dive**: Leverage CERS data for comprehensive CA analysis
    5. **Competitive Analysis**: Compare T-Mobile vs AT&T permit tracking (reference case)

    **Value Proposition**: Demonstrate missed compliance items and consolidation needs
    """
    )
    return


if __name__ == "__main__":
    app.run()
