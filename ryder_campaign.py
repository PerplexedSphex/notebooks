import marimo

__generated_with = "0.14.11"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md(
        r"""
        ## Data Dependencies Setup
        
        **Purpose**: Import all required libraries for data analysis and visualization
        
        **Components**:
        - `duckdb`: In-memory SQL database for fast data processing
        - `plotly`: Interactive visualization library
        - `pandas`: Data manipulation and analysis
        - `pathlib`: File system path handling
        
        This cell establishes the foundation for our logistics compliance analysis.
        """
    )
    return


@app.cell
def _():
    import marimo as mo
    import plotly.express as px
    import pandas as pd
    from pathlib import Path
    return Path, mo, pd, px


@app.cell
def _(mo):
    mo.md(
        r"""
    # Ryder Logistics Campaign Analysis

    **Campaign Focus**: Ryder waste compliance and logistics facility analysis
    **Deadline**: End of next week (Jenny)
    **Value**: ~$300k deals in play, national story starting with California
    **Approach**: Waste dataset connection to Encamp services value proposition
    """
    )
    return


@app.cell
def _(mo):
    mo.md(
        r"""
        ## Database Connection Setup
        
        **Purpose**: Connect to existing RCRA DuckDB database with processed EPA data
        
        **Why This Approach**:
        - Use persistent database instead of loading CSVs each time
        - Pre-processed and optimized data structure for faster queries
        - Consistent data access across all campaign notebooks
        
        **Database Location**: `~/db/rcrainfo.duckdb` contains:
        - RCRA handlers and facilities
        - Violation and enforcement data  
        - Waste manifests and tracking
        - Optimized indexes for logistics analysis
        
        **Business Value**: Direct access to cleaned, structured EPA data for immediate Ryder compliance analysis.
        """
    )
    return


@app.cell
def _(Path, mo, pd, px):
    import duckdb
    
    # Connect to existing RCRA database
    RCRA_DB_PATH = Path("~/db/rcrainfo.duckdb").expanduser()
    rcra_con = duckdb.connect(database=RCRA_DB_PATH, read_only=True)
    
    print(f"Connected to: {RCRA_DB_PATH}")
    
    handler_count = rcra_con.execute('SELECT COUNT(*) FROM HD_HANDLER').fetchone()[0]
    print(f"Database loaded with {handler_count} handlers")
    
    return RCRA_DB_PATH, duckdb, rcra_con


@app.cell
def _(mo):
    mo.md(
        r"""
        ## Ryder Facility Discovery
        
        **Purpose**: Identify all Ryder-related facilities in EPA databases
        
        **Search Strategy**:
        - Pattern matching on facility names using LIKE operations
        - Multiple search terms to capture naming variations
        - Comprehensive facility data extraction for location and status
        
        **Key Fields**:
        - `HANDLER_NAME`: Official facility name in EPA records
        - `LOCATION_*`: Physical address for site mapping
        - `HANDLER_TYPE`: Regulatory classification (generator, transporter, etc.)
        - `GENERATOR_STATUS`: Waste generation volume category
        
        **Business Application**: This forms the baseline inventory of Ryder facilities under EPA oversight, critical for compliance gap analysis.
        """
    )
    return


@app.cell
def _(mo, rcra_con):
    # Initial Ryder facility identification using marimo SQL
    ryder_facilities = mo.sql(
        """
        SELECT 
            handler_name,
            mail_street1,
            location_city,
            location_state,
            location_zip,
            activity_location,
            CASE 
                WHEN fed_waste_generator = 'Y' THEN 'Generator'
                WHEN transporter = 'Y' THEN 'Transporter'
                WHEN tsd_activity = 'Y' THEN 'TSD Facility'
                ELSE 'Other'
            END as handler_type,
            CASE
                WHEN lqhuw = 'Y' THEN 'Large Quantity Generator'
                WHEN fed_waste_generator = 'Y' THEN 'Small Quantity Generator' 
                ELSE 'Non-Generator'
            END as generator_status
        FROM HD_HANDLER
        WHERE 
            UPPER(handler_name) LIKE '%RYDER%'
            OR UPPER(handler_name) LIKE '%RYDER TRUCK%'
            OR UPPER(handler_name) LIKE '%RYDER SYSTEM%'
        ORDER BY location_state, location_city
        """,
        engine=rcra_con
    )

    mo.ui.table(
        ryder_facilities,
        label="Ryder Facilities in RCRA Database"
    )
    return ryder_facilities,


@app.cell
def _(mo):
    mo.md(
        r"""
        ## California Market Focus Analysis
        
        **Strategic Rationale**: 
        - California represents largest regulatory burden for logistics companies
        - State-level environmental requirements exceed federal minimums
        - High-value market justifies focused compliance investment
        
        **Methodology**:
        - Filter national Ryder data to California operations
        - Establish baseline metrics for expansion analysis
        - Demonstrate market penetration and opportunity size
        
        **Sales Context**: California analysis provides proof-of-concept for national rollout, showing immediate ROI potential in highest-stakes market.
        """
    )
    return


@app.cell
def _(mo, ryder_facilities):
    # California focus and waste compliance analysis
    # Convert to pandas if needed for filtering
    rf_pandas = ryder_facilities.to_pandas() if hasattr(ryder_facilities, 'to_pandas') else ryder_facilities
    ca_ryder = rf_pandas[rf_pandas['location_state'] == 'CA']

    mo.md(f"""
    ## California Ryder Facilities Focus

    **Total Ryder facilities found**: {len(rf_pandas)}
    **California Ryder facilities**: {len(ca_ryder)}

    Starting with California for detailed waste compliance analysis, building to national story.
    """)
    return


@app.cell
def _(mo):
    mo.md(
        r"""
        ## Industry-Wide Compliance Analysis
        
        **Purpose**: Benchmark Ryder against logistics industry compliance patterns
        
        **Competitive Intelligence Strategy**:
        - Include major logistics competitors (FedEx, UPS) for market context
        - Rank facilities by violation frequency to identify problem patterns
        - Cross-reference handler types with violation rates
        
        **Query Logic**:
        - LEFT JOIN preserves facilities with zero violations
        - COUNT DISTINCT prevents duplicate violation counting
        - ORDER BY violation_count surfaces highest-risk facilities first
        
        **Sales Opportunity**: Demonstrates industry-wide compliance challenges, positioning Encamp as solution for systematic risk management across logistics sector.
        """
    )
    return


@app.cell
def _(mo, rcra_con):
    # Waste compliance and violation analysis using marimo SQL with actual violation data
    logistics_waste_compliance = mo.sql(
        """
        SELECT 
            h.handler_name,
            h.location_state,
            h.location_city,
            CASE
                WHEN h.lqhuw = 'Y' THEN 'Large Quantity Generator'
                WHEN h.fed_waste_generator = 'Y' THEN 'Small Quantity Generator' 
                ELSE 'Non-Generator'
            END as generator_status,
            CASE 
                WHEN h.fed_waste_generator = 'Y' THEN 'Generator'
                WHEN h.transporter = 'Y' THEN 'Transporter'
                WHEN h.tsd_activity = 'Y' THEN 'TSD Facility'
                ELSE 'Other'
            END as handler_type,
            COUNT(DISTINCT h.handler_id) as facility_count,
            COUNT(DISTINCT ca.event_handler_id) as violation_events,
            CASE 
                WHEN COUNT(DISTINCT ca.event_handler_id) > 0 
                THEN ROUND(COUNT(DISTINCT ca.event_handler_id)::FLOAT / COUNT(DISTINCT h.handler_id), 3)
                ELSE 0
            END as violation_rate
        FROM HD_HANDLER h
        LEFT JOIN CA_AREA_EVENT ca ON h.handler_id = ca.area_handler_id
        WHERE 
            UPPER(h.handler_name) LIKE '%LOGISTICS%'
            OR UPPER(h.handler_name) LIKE '%FREIGHT%'
            OR UPPER(h.handler_name) LIKE '%SHIPPING%'
            OR UPPER(h.handler_name) LIKE '%TRANSPORT%'
            OR UPPER(h.handler_name) LIKE '%RYDER%'
            OR UPPER(h.handler_name) LIKE '%FEDEX%'
            OR UPPER(h.handler_name) LIKE '%UPS%'
        GROUP BY h.handler_name, h.location_state, h.location_city, generator_status, handler_type
        HAVING COUNT(DISTINCT h.handler_id) > 0
        ORDER BY violation_rate DESC, facility_count DESC
        """,
        engine=rcra_con
    )

    mo.ui.table(
        logistics_waste_compliance.head(20),
        label="Logistics Industry Waste Compliance Overview"
    )
    return logistics_waste_compliance,


@app.cell
def _(mo):
    mo.md(
        r"""
        ## Waste Shipment Tracking Analysis
        
        **Purpose**: Analyze Ryder's waste transportation and disposal patterns
        
        **Manifest System Context**:
        - EPA requires manifests for hazardous waste "cradle-to-grave" tracking
        - Each manifest represents legal liability chain from generator to disposal
        - Missing or incorrect manifests create significant regulatory exposure
        
        **Analysis Approach**:
        - Track Ryder as both generator (producing waste) and transporter (moving waste)
        - Quantify waste volumes and shipment frequencies
        - Map disposal facility relationships and geographic patterns
        
        **Encamp Value Proposition**: Automated manifest tracking prevents regulatory violations and optimizes disposal cost management through better facility relationships.
        """
    )
    return


@app.cell
def _(mo, rcra_con):
    # Manifest analysis for waste shipment patterns using marimo SQL
    ryder_manifests = mo.sql(
        """
        SELECT 
            m.generator_name,
            m.generator_location_state,
            m.des_facility_name,
            m.des_fac_location_state,
            COUNT(*) as manifest_count,
            SUM(CAST(m.total_quantity_kg AS DOUBLE)) as total_quantity_kg
        FROM EM_MANIFEST m
        WHERE 
            UPPER(m.generator_name) LIKE '%RYDER%'
            OR UPPER(m.des_facility_name) LIKE '%RYDER%'
        GROUP BY m.generator_name, m.generator_location_state, m.des_facility_name, m.des_fac_location_state
        ORDER BY manifest_count DESC
        """,
        engine=rcra_con
    )

    mo.ui.table(
        ryder_manifests,
        label="Ryder Waste Manifest Activity"
    )
    return ryder_manifests,


@app.cell
def _(mo):
    mo.md(
        r"""
        ## Geographic Risk Visualization
        
        **Purpose**: Map state-level compliance risk across logistics industry
        
        **Visualization Strategy**:
        - Scatter plot reveals correlation between facility density and violations
        - State labels enable quick identification of high-risk markets
        - Interactive plotting allows drill-down analysis
        
        **Business Insights**:
        - States with high facility counts but low violations indicate effective compliance
        - High violation states represent market opportunities for Encamp services
        - Geographic patterns suggest regulatory enforcement variations
        
        **Sales Application**: Identify priority states for Encamp expansion based on demonstrated compliance challenges and market size.
        """
    )
    return


@app.cell
def _(logistics_waste_compliance, mo, px):
    # Visualization: State distribution of logistics facilities
    # Convert to pandas for visualization if needed
    df_pandas = logistics_waste_compliance.to_pandas() if hasattr(logistics_waste_compliance, 'to_pandas') else logistics_waste_compliance
    
    state_counts = df_pandas.groupby('location_state').agg({
        'handler_name': 'count',
        'facility_count': 'sum'
    }).reset_index()
    state_counts.columns = ['State', 'Unique_Facilities', 'Total_Facilities']

    fig = px.bar(
        state_counts.head(15), 
        x='State', 
        y='Total_Facilities',
        title="Logistics Facilities by State (Top 15)",
        labels={'Total_Facilities': 'Number of Facilities'}
    )

    mo.ui.plotly(fig)
    return


@app.cell
def _(mo):
    mo.md(
        r"""
        ## Waste Generation Profile & Risk Assessment
        
        **Purpose**: Analyze compliance rates by waste generation volume categories
        
        **Generator Status Categories**:
        - **Large Quantity Generators (LQG)**: >1,000 kg/month - highest regulatory burden
        - **Small Quantity Generators (SQG)**: 100-1,000 kg/month - moderate requirements  
        - **Conditionally Exempt Small Quantity Generators (CESQG)**: <100 kg/month - minimal requirements
        
        **Risk Analysis Methodology**:
        - Calculate violation rates by generator category
        - Identify patterns between waste volume and compliance failures
        - Quantify facility counts for market sizing
        
        **Strategic Value**: Different generator categories require different Encamp service levels, enabling tiered pricing and targeted sales approaches.
        """
    )
    return


@app.cell
def _(mo, rcra_con):
    # Generator status analysis for compliance gaps using marimo SQL with actual violation rates
    generator_analysis = mo.sql(
        """
        SELECT 
            CASE
                WHEN h.lqhuw = 'Y' THEN 'Large Quantity Generator'
                WHEN h.fed_waste_generator = 'Y' THEN 'Small Quantity Generator' 
                ELSE 'Non-Generator'
            END as generator_status,
            CASE 
                WHEN h.fed_waste_generator = 'Y' THEN 'Generator'
                WHEN h.transporter = 'Y' THEN 'Transporter'
                WHEN h.tsd_activity = 'Y' THEN 'TSD Facility'
                ELSE 'Other'
            END as handler_type,
            COUNT(DISTINCT h.handler_id) as facility_count,
            COUNT(DISTINCT ca.event_handler_id) as facilities_with_violations,
            CASE 
                WHEN COUNT(DISTINCT h.handler_id) > 0
                THEN ROUND(COUNT(DISTINCT ca.event_handler_id)::FLOAT / COUNT(DISTINCT h.handler_id) * 100, 1)
                ELSE 0
            END as violation_rate_percent
        FROM HD_HANDLER h
        LEFT JOIN CA_AREA_EVENT ca ON h.handler_id = ca.area_handler_id
        WHERE 
            UPPER(h.handler_name) LIKE '%LOGISTICS%'
            OR UPPER(h.handler_name) LIKE '%FREIGHT%'
            OR UPPER(h.handler_name) LIKE '%TRANSPORT%'
            OR UPPER(h.handler_name) LIKE '%RYDER%'
        GROUP BY generator_status, handler_type
        HAVING COUNT(DISTINCT h.handler_id) > 0
        ORDER BY violation_rate_percent DESC, facility_count DESC
        """,
        engine=rcra_con
    )

    mo.ui.table(
        generator_analysis,
        label="Logistics Waste Generator Status & Compliance Rates"
    )
    return generator_analysis,


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Next Analysis Steps

    1. **California Deep Dive**: Pull all Ryder CA facilities and compliance gaps
    2. **Waste Stream Analysis**: Identify specific waste types and management patterns
    3. **Compliance Gaps**: Map missing permits, late reports, violations
    4. **National Expansion**: Scale analysis beyond California using patterns found
    5. **Value Quantification**: Connect compliance gaps to Encamp service offerings

    **Encamp Value Proposition**: 
    - Waste manifest tracking and compliance
    - Multi-state permit management
    - Violation monitoring and response
    - Cost savings through consolidated compliance view

    **Target Deal Size**: ~$300k based on national logistics operations scope
    """
    )
    return


if __name__ == "__main__":
    app.run()
