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
    # RCRA Account Analysis Dashboard
    
    Analysis of EPA RCRA (Resource Conservation and Recovery Act) data by organizational domain.
    Focus on hazardous waste management, facility compliance, and regulatory tracking.
    """
    )
    return


@app.cell
def _():
    import duckdb
    import pandas as pd
    import plotly.express as px
    import plotly.graph_objects as go
    from pathlib import Path
    
    # Connect to RCRA database
    RCRA_DB_PATH = Path("~/db/rcrainfo.duckdb").expanduser()
    rcra_con = duckdb.connect(database=RCRA_DB_PATH, read_only=True)
    
    print("✅ Connected to RCRA database")
    print(f"Database: {RCRA_DB_PATH}")
    return RCRA_DB_PATH, duckdb, go, pd, px, rcra_con


@app.cell
def _(mo, rcra_con):
    # First, let's see what tables we have
    tables_query = mo.sql(
        """
        PRAGMA show_tables
        """,
        engine=rcra_con
    )
    
    mo.md(f"**Available RCRA Tables:** {len(tables_query)} total")
    return (tables_query,)


@app.cell
def _(mo, rcra_con):
    # Find domain information from handler contacts
    rcra_domains = mo.sql(
        """
        WITH handler_emails AS (
            SELECT DISTINCT 
                handler_id,
                contact_email_address as email
            FROM HD_HANDLER 
            WHERE current_record = 'Y'
            AND contact_email_address IS NOT NULL
            AND contact_email_address != ''
            AND contact_email_address LIKE '%@%'
        ),
        cert_emails AS (
            SELECT DISTINCT
                handler_id,
                cert_email as email
            FROM HD_CERTIFICATION
            WHERE cert_email IS NOT NULL
            AND cert_email != ''
            AND cert_email LIKE '%@%'
        ),
        all_emails AS (
            SELECT * FROM handler_emails
            UNION 
            SELECT * FROM cert_emails
        )
        SELECT DISTINCT
            handler_id,
            split_part(email, '@', 2) as contact_domain
        FROM all_emails
        WHERE split_part(email, '@', 2) != ''
        AND split_part(email, '@', 2) IS NOT NULL
        ORDER BY contact_domain
        """,
        engine=rcra_con
    )
    
    mo.md(f"**RCRA Domain Extraction Complete:** {len(rcra_domains)} handler-domain pairs found")
    return (rcra_domains,)


@app.cell
def _(mo, rcra_domains):
    # Get top domains for dropdown using polars syntax
    top_domains = (
        rcra_domains
        .group_by('contact_domain')
        .len()
        .sort('len', descending=True)
        .head(20)
        .get_column('contact_domain')
        .to_list()
    )
    
    # Domain selector UI with default value
    prospect_domain = mo.ui.text(
        value="amazon.com",  # Default value
        placeholder="Enter domain name (e.g., company.com)", 
        label="Target Organization Domain"
    )
    
    # Dropdown for common domains
    domain_dropdown = mo.ui.dropdown(
        options=[""] + top_domains,
        value="",
        label="Or select from top domains"
    )
    
    mo.md("## Account Analysis by Domain")
    return (prospect_domain, domain_dropdown, top_domains)


@app.cell
def _(mo, prospect_domain, domain_dropdown):
    # Display the selectors
    mo.hstack([prospect_domain, domain_dropdown])
    return


@app.cell
def _(prospect_domain, domain_dropdown, rcra_domains, rcra_con):
    # Filter handlers by domain - dropdown overrides text input
    selected_domain = domain_dropdown.value if domain_dropdown.value else prospect_domain.value
    
    if selected_domain:
        target_domain = selected_domain.upper().strip()
        
        # Filter the domains data using polars syntax
        filtered_handlers = (
            rcra_domains
            .filter(rcra_domains['contact_domain'].str.to_uppercase() == target_domain)
        )
        
        # Register with DuckDB for further queries
        rcra_con.register("target_handlers", filtered_handlers.select("handler_id"))
        
        print(f"🎯 Found {len(filtered_handlers)} handlers for domain: {target_domain}")
        print(f"Handler IDs: {filtered_handlers['handler_id'].to_list()[:5]}{'...' if len(filtered_handlers) > 5 else ''}")
    else:
        print("💡 Enter a domain name above to start analysis")
    return (selected_domain,)


@app.cell
def _(mo, selected_domain, rcra_con):
    # Basic facility information for selected domain
    if selected_domain:
        facility_info = mo.sql(
            """
            SELECT 
                h.handler_id,
                h.handler_name,
                h.location_street1,
                h.location_city,
                h.location_state,
                h.location_zip,
                h.fed_waste_generator,
                h.contact_email_address
            FROM HD_HANDLER h
            WHERE h.handler_id IN (SELECT handler_id FROM target_handlers)
            AND h.current_record = 'Y'
            ORDER BY h.handler_name
            """,
            engine=rcra_con
        )
        
        mo.md(f"### 🏢 Facilities Overview\n**{len(facility_info)} facilities found**")
    else:
        facility_info = pd.DataFrame()
        mo.md("### 🏢 Facilities Overview\n*Select a domain to see facilities*")
    return (facility_info,)


@app.cell
def _(facility_info, mo, selected_domain):
    # Display facility table
    if selected_domain and len(facility_info) > 0:
        mo.ui.table(facility_info, pagination=True)
    return


@app.cell  
def _(mo, selected_domain, rcra_con):
    # Generator status breakdown
    if selected_domain:
        status_breakdown = mo.sql(
            """
            SELECT 
                h.fed_waste_generator,
                COUNT(*) as facility_count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage
            FROM HD_HANDLER h
            WHERE h.handler_id IN (SELECT handler_id FROM target_handlers)
            AND h.current_record = 'Y'
            AND h.fed_waste_generator IS NOT NULL
            GROUP BY h.fed_waste_generator
            ORDER BY facility_count DESC
            """,
            engine=rcra_con
        )
        
        mo.md("### 📊 Generator Status Distribution")
    else:
        status_breakdown = pd.DataFrame()
    return (status_breakdown,)


@app.cell
def _(mo, selected_domain, status_breakdown, px):
    # Visualize status breakdown
    if selected_domain and len(status_breakdown) > 0:
        fig = px.pie(
            status_breakdown, 
            values='facility_count', 
            names='fed_waste_generator',
            title=f'RCRA Generator Status for {selected_domain}'
        )
        
        mo.ui.plotly(fig)
    return


@app.cell
def _(mo, selected_domain, rcra_con):
    # Biennial Report data - waste generation tracking
    if selected_domain:
        waste_summary = mo.sql(
            """
            SELECT 
                br.report_cycle,
                COUNT(DISTINCT br.handler_id) as facilities_reporting,
                COUNT(*) as total_waste_records
            FROM BR_REPORTING_2023 br  -- Latest biennial report
            WHERE br.handler_id IN (SELECT handler_id FROM target_handlers)
            GROUP BY br.report_cycle
            ORDER BY br.report_cycle DESC
            """,
            engine=rcra_con
        )
        
        mo.md("### 📈 Waste Generation Reporting (2023)")
    else:
        waste_summary = pd.DataFrame()
    return (waste_summary,)


@app.cell
def _(mo, selected_domain, waste_summary):
    # Display waste summary
    if selected_domain and len(waste_summary) > 0:
        mo.ui.table(waste_summary)
    elif selected_domain:
        mo.md("*No biennial reporting data found for this domain*")
    return


@app.cell
def _(mo, selected_domain, rcra_con):
    # Enforcement actions and violations
    if selected_domain:
        enforcement_summary = mo.sql(
            """
            SELECT 
                COUNT(*) as total_evaluations,
                COUNT(DISTINCT ce.handler_id) as facilities_evaluated
            FROM CE_REPORTING ce
            WHERE ce.handler_id IN (SELECT handler_id FROM target_handlers)
            """,
            engine=rcra_con
        )
        
        mo.md("### ⚖️ Compliance & Enforcement Summary")
    else:
        enforcement_summary = pd.DataFrame()
    return (enforcement_summary,)


@app.cell
def _(enforcement_summary, mo, selected_domain):
    # Display enforcement summary  
    if selected_domain and len(enforcement_summary) > 0:
        mo.ui.table(enforcement_summary)
    elif selected_domain:
        mo.md("*No enforcement data found for this domain*")
    return


if __name__ == "__main__":
    app.run()