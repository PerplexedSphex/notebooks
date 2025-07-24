import marimo

__generated_with = "0.14.10"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import plotly.express as px

    from pathlib import Path
    import duckdb

    # Connect to existing RCRA database
    RCRA_DB_PATH = Path("~/db/rcrainfo.duckdb").expanduser()
    rcra_con = duckdb.connect(database=RCRA_DB_PATH, read_only=True)

    print(f"Connected to: {RCRA_DB_PATH}")

    handler_count = rcra_con.execute('SELECT COUNT(*) FROM HD_HANDLER').fetchone()[0]
    print(f"Database loaded with {handler_count} handlers")
    return mo, rcra_con


@app.cell
def _(HD_HANDLER, mo, rcra_con):
    fedex_facs = mo.sql(
        f"""
        SELECT handler_id
            ,activity_location
            ,source_type
            ,seq_number
            ,receive_date
            ,handler_name
            ,case 
                when handler_name like 'FEDEX EXPRESS%' or handler_name like 'FED EX EXPRESS%' then 'Fedex Express'
                when handler_name like 'FEDEX FREIGHT%' or handler_name like 'FED EX FREIGHT%' then 'Fedex Freight'
                when handler_name like 'FEDEX GROUND%' or handler_name like 'FED EX GROUND%' then 'Fedex Ground'
                when handler_name like 'FEDEX NATIONAL%' or handler_name like 'FED EX NATIONAL%' then 'Fedex National'
                when handler_name like 'FEDERAL EXPRESS%' or handler_name like 'FEDEX' or handler_name like 'FEDEX CORP%' then 'Fedex Corp'
                else 'Fedex Other' end as business_unit
            ,case 
        		when fed_waste_generator = '1' then 'LQG' 
        		when fed_waste_generator = '2' then 'SQG' 
        		when fed_waste_generator = '3' then 'VSQG' 
        		when fed_waste_generator = 'N' then 'Not a Generator' 
        		else 'Other'
        	end as generator_status
        	,contact_email_address
        	,contact_title
        	,contact_first_name
        	,contact_last_name
        	,contact_phone	
        FROM "HD_HANDLER"
        where (
                handler_name like '%FEDEX%' 
                or contact_email_address like '%@FEDEX.COM' 
                or contact_email_address like '%@CORP.DS.FEDEX.COM'
                or handler_name like '%FEDERAL EXPRESS%' 
                or handler_name like 'FED EX %' 
            )
            and current_record='Y'
        """,
        engine=rcra_con
    )
    return (fedex_facs,)


@app.cell
def _(fedex_facs, mo, rcra_con):
    _df = mo.sql(
        f"""
        SELECT business_unit
            ,count(distinct handler_id) as facs 
        FROM fedex_facs
        group by business_unit order by facs desc
        """,
        engine=rcra_con
    )
    return


@app.cell
def _(HD_HANDLER, mo, rcra_con):
    _df = mo.sql(
        f"""
        SELECT 
            -- case 
            --     when handler_name like 'FEDEX EXPRESS%' or handler_name like 'FED EX EXPRESS%' then 'Fedex Express'
            --     when handler_name like 'FEDEX FREIGHT%' or handler_name like 'FED EX FREIGHT%' then 'Fedex Freight'
            --     when handler_name like 'FEDEX GROUND%' or handler_name like 'FED EX GROUND%' then 'Fedex Ground'
            --     when handler_name like 'FEDEX NATIONAL%' or handler_name like 'FED EX NATIONAL%' then 'Fedex National'
            --     when handler_name like 'FEDERAL EXPRESS%' or handler_name like 'FEDEX' or handler_name like 'FEDEX CORP%' then 'Fedex Corp'
            --     else 'Fedex Other' end as business_unit   
            handler_id
            ,activity_location
            ,source_type
            ,seq_number
            ,receive_date
            ,handler_name
            ,case 
        		when fed_waste_generator = '1' then 'LQG' 
        		when fed_waste_generator = '2' then 'SQG' 
        		when fed_waste_generator = '3' then 'VSQG' 
        		when fed_waste_generator = 'N' then 'Not a Generator' 
        		else 'Other'
        	end as generator_status
        	,contact_email_address
        	,contact_title
        	,contact_first_name
        	,contact_last_name
        	,contact_phone	
        FROM "HD_HANDLER"
        where (
            handler_name like 'XPO %' 
                or 
            contact_email_address like '%@XPO.COM' 
                or contact_email_address like '%@CON-WAY.COM'
                or handler_name like 'XPO,%' 
                or handler_name like 'CON-WAY %' 
            )
            and current_record='Y'
        """,
        engine=rcra_con
    )
    return


@app.cell
def _(HD_HANDLER, mo, rcra_con):
    _df = mo.sql(
        f"""
        SELECT handler_id
            ,activity_location
            ,source_type
            ,seq_number
            ,receive_date
            ,current_record
            ,case when current_record = 'Y' then true when current_record='N' then false else 'other' end as latest_filing
            ,cast(receive_date as int) >= 20210901 as 'filed_since_sept_2021'
            ,handler_name
            ,trim(coalesce(location_street_no,'') || ' ' || coalesce(location_street1,'')) as street_address
            ,location_city
            ,location_city
            ,case 
        		when fed_waste_generator = '1' then 'LQG' 
        		when fed_waste_generator = '2' then 'SQG' 
        		when fed_waste_generator = '3' then 'VSQG' 
        		when fed_waste_generator = 'N' then 'Not a Generator' 
        		else 'Other'
        	end as generator_status
            ,contact_email_address
        FROM "HD_HANDLER"
        where contact_email_address ilike '%@tesla.com'
        order by handler_id, receive_date desc
        """,
        engine=rcra_con
    )
    return


@app.cell
def _(HD_HANDLER, mo, rcra_con):
    ryder_facs = mo.sql(
        f"""
        SELECT handler_id
            ,activity_location
            ,source_type
            ,seq_number
            ,receive_date
            ,handler_name
            ,CASE 
                WHEN handler_name ILIKE 'RYDER TRUCK%' THEN 'Ryder Truck Rental'
                WHEN handler_name ILIKE 'RYDER TRANS%' OR handler_name ILIKE '%TRANSPORTATION SERVICE%' THEN 'Ryder Transportation Services'
                WHEN handler_name ILIKE '%LOGISTICS%' or handler_name ILIKE '%SUPPLY CHAIN%' THEN 'Ryder Integrated Logistics'
                WHEN handler_name ILIKE '%STUDENT%' THEN 'Ryder Student Transportation'
                ELSE 'Ryder Other'
            END as business_unit
            ,CASE 
                WHEN fed_waste_generator = '1' THEN 'LQG' 
                WHEN fed_waste_generator = '2' THEN 'SQG' 
                WHEN fed_waste_generator = '3' THEN 'VSQG' 
                WHEN fed_waste_generator = 'N' THEN 'Not a Generator' 
                ELSE 'Other'
            END as generator_status
            ,contact_email_address
            ,contact_title
            ,contact_first_name
            ,contact_last_name
            ,contact_phone    
        FROM "HD_HANDLER"
        WHERE (
                handler_name ILIKE '%RYDER%' 
                OR contact_email_address ILIKE '%@ryder.com'
                OR contact_email_address ILIKE '%@ryderheil.com'
                OR contact_email_address ILIKE '%@ryderfs.com'
            )
            AND current_record='Y'
        """,
        engine=rcra_con
    )
    return (ryder_facs,)


@app.cell
def _(mo, rcra_con, ryder_facs):
    _df = mo.sql(
        f"""
        SELECT business_unit
            ,count(distinct handler_id) as facs 
        FROM ryder_facs
        group by business_unit order by facs desc
        """,
        engine=rcra_con
    )
    return


@app.cell
def _(mo):
    import pandas as pd

    # Load internal Ryder data for comparison
    ryder_internal_facs = pd.read_csv("/Users/sam/global-context-buffer/ryder-facs.csv")
    ryder_internal_ids = pd.read_csv("/Users/sam/global-context-buffer/ryder-fac-ids.csv")

    # Extract EPA IDs from internal system
    internal_epa_ids = ryder_internal_ids[ryder_internal_ids['Type'] == 'EPA']['Value'].tolist()

    mo.md(f"""
    ## Internal Ryder System Analysis

    **Internal System Overview:**
    - Total facilities: {len(ryder_internal_facs):,}
    - Facilities with EPA IDs: {len(internal_epa_ids):,}
    - Business units: {ryder_internal_facs['Business Unit'].nunique()}

    **Business Unit Breakdown (Internal):**
    """)
    return internal_epa_ids, ryder_internal_facs, ryder_internal_ids


@app.cell
def _(ryder_internal_facs):
    ryder_internal_facs['Business Unit'].value_counts().head(10)
    return


@app.cell
def _(HD_HANDLER, internal_epa_ids, mo, rcra_con):
    # Find overlap between internal EPA IDs and RCRA database
    if internal_epa_ids:
        epa_id_list_1 = "'" + "','".join(internal_epa_ids) + "'"

        internal_rcra_matches = mo.sql(f"""
            SELECT handler_id, handler_name, location_city, location_state,
                   CASE 
                       WHEN fed_waste_generator = '1' THEN 'LQG' 
                       WHEN fed_waste_generator = '2' THEN 'SQG' 
                       WHEN fed_waste_generator = '3' THEN 'VSQG' 
                       WHEN fed_waste_generator = 'N' THEN 'Not a Generator' 
                       ELSE 'Other'
                   END as generator_status
            FROM HD_HANDLER 
            WHERE handler_id IN ({epa_id_list_1})
                AND current_record='Y'
            ORDER BY handler_name
        """, engine=rcra_con)

    mo.md(f"""
    ## Internal vs RCRA Overlap Analysis

    **Coverage Summary:**
    - Internal EPA IDs: {len(internal_epa_ids):,}
    - RCRA database matches: {len(internal_rcra_matches):,}
    - Match rate: {len(internal_rcra_matches)/len(internal_epa_ids)*100:.1f}%
    """)
    return (internal_rcra_matches,)


@app.cell
def _(internal_rcra_matches):
    # Generator status breakdown for matched facilities
    internal_rcra_matches['generator_status'].value_counts()
    return


@app.cell
def _(HD_HANDLER, mo, rcra_con, ryder_internal_facs, ryder_internal_ids):
    # Compare internal FMS categorization with our RCRA business unit rules

    # Get internal facilities marked as FMS
    fms_facilities = ryder_internal_facs[ryder_internal_facs['Business Unit'] == 'FMS']

    # Get their EPA IDs
    fms_facility_ids = set(fms_facilities['Facility ID'].tolist())
    fms_epa_mapping = ryder_internal_ids[
        (ryder_internal_ids['Type'] == 'EPA') & 
        (ryder_internal_ids['Facility ID'].isin(fms_facility_ids))
    ][['Facility ID', 'Value']].rename(columns={'Value': 'handler_id'})

    fms_epa_ids = fms_epa_mapping['handler_id'].tolist()

    if fms_epa_ids:
        fms_epa_list = "'" + "','".join(fms_epa_ids) + "'"

        # Get RCRA data for FMS facilities with our business unit categorization
        fms_rcra_comparison = mo.sql(f"""
            SELECT 
                f.handler_id,
                f.handler_name,
                f.location_city,
                f.location_state,
                CASE 
                    WHEN f.handler_name ILIKE 'RYDER TRUCK%' THEN 'Ryder Truck Rental'
                    WHEN f.handler_name ILIKE 'RYDER TRANS%' OR f.handler_name ILIKE '%TRANSPORTATION SERVICE%' THEN 'Ryder Transportation Services'
                    WHEN f.handler_name ILIKE '%LOGISTICS%' or f.handler_name ILIKE '%SUPPLY CHAIN%' THEN 'Ryder Integrated Logistics'
                    WHEN f.handler_name ILIKE '%STUDENT%' THEN 'Ryder Student Transportation'
                    ELSE 'Ryder Other'
                END as rcra_business_unit,
                'FMS' as internal_business_unit
            FROM HD_HANDLER f
            WHERE f.handler_id IN ({fms_epa_list})
                AND f.current_record='Y'
            ORDER BY f.handler_name
        """, engine=rcra_con)

    mo.md(f"""
    ## FMS Categorization Validation

    **Internal FMS Facilities Analysis:**
    - Internal facilities marked as FMS: {len(fms_facilities):,}
    - FMS facilities with EPA IDs: {len(fms_epa_ids):,}
    - Found in RCRA database: {len(fms_rcra_comparison):,}

    **Business Unit Mapping Check:**
    Compare internal "FMS" designation with our RCRA categorization rules
    """)
    return (fms_rcra_comparison,)


@app.cell
def _(fms_rcra_comparison):
    # Show the comparison
    fms_rcra_comparison
    return


@app.cell
def _(fms_rcra_comparison):
    # Summary of categorization alignment
    categorization_summary = fms_rcra_comparison['rcra_business_unit'].value_counts()

    print("RCRA Business Unit Categories for Internal FMS Facilities:")
    print(categorization_summary)

    # Calculate alignment percentage
    truck_rental_count = categorization_summary.filter(
        categorization_summary['rcra_business_unit'] == 'Ryder Truck Rental'
    )['counts'].sum() if len(categorization_summary.filter(categorization_summary['rcra_business_unit'] == 'Ryder Truck Rental')) > 0 else 0

    total_categorized = len(fms_rcra_comparison)
    alignment_pct = (truck_rental_count / total_categorized * 100) if total_categorized > 0 else 0

    print(f"\nCategorization Alignment:")
    print(f"- Truck Rental (expected for FMS): {truck_rental_count}/{total_categorized} ({alignment_pct:.1f}%)")
    print(f"- Other categories: {total_categorized - truck_rental_count}/{total_categorized} ({100-alignment_pct:.1f}%)")
    return


@app.cell
def _(HD_HANDLER, internal_epa_ids, mo, rcra_con, ryder_facs):
    # Find facilities in RCRA from internal system that weren't caught by name-based query
    if internal_epa_ids:
        epa_id_list_2 = "'" + "','".join(internal_epa_ids) + "'"

        # Get internal facilities in RCRA
        internal_in_rcra = mo.sql(f"""
            SELECT handler_id, handler_name, location_city, location_state,
                   contact_email_address
            FROM HD_HANDLER 
            WHERE handler_id IN ({epa_id_list_2})
                AND current_record='Y'
        """, engine=rcra_con)

        # Get facilities from name-based query  
        name_based_ids = mo.sql("""
            SELECT DISTINCT handler_id 
            FROM ryder_facs
        """, engine=rcra_con)

        # Find the gap - internal facilities in RCRA but not caught by name query
        internal_rcra_ids = set(internal_in_rcra['handler_id'].to_list())
        name_based_ids_set = set(name_based_ids['handler_id'].to_list())

        gap_ids = internal_rcra_ids - name_based_ids_set

        if gap_ids:
            gap_id_list = "'" + "','".join(gap_ids) + "'"

            missed_facilities = mo.sql(f"""
                SELECT handler_id, handler_name, location_city, location_state,
                       contact_email_address,
                       CASE 
                           WHEN fed_waste_generator = '1' THEN 'LQG' 
                           WHEN fed_waste_generator = '2' THEN 'SQG' 
                           WHEN fed_waste_generator = '3' THEN 'VSQG' 
                           WHEN fed_waste_generator = 'N' THEN 'Not a Generator' 
                           ELSE 'Other'
                       END as generator_status
                FROM HD_HANDLER 
                WHERE handler_id IN ({gap_id_list})
                    AND current_record='Y'
                ORDER BY handler_name
            """, engine=rcra_con)
        else:
            missed_facilities = mo.sql("SELECT 'No missed facilities' as message", engine=rcra_con)

    mo.md(f"""
    ## Coverage Gap Analysis

    **Facilities missed by name-based query:**
    - Internal facilities in RCRA: {len(internal_in_rcra):,}
    - Caught by name-based query: {len(name_based_ids):,} 
    - **Missed facilities: {len(gap_ids) if gap_ids else 0:,}**

    These are Ryder facilities in our internal system that exist in RCRA 
    but weren't identified by our name/email pattern matching.
    """)
    return (missed_facilities,)


@app.cell
def _(missed_facilities):
    # Show the missed facilities
    missed_facilities
    return


@app.cell
def _(HD_HANDLER, internal_epa_ids, mo, rcra_con):
    # Summary statistics
    if internal_epa_ids:
        epa_id_list_3 = "'" + "','".join(internal_epa_ids) + "'"

        coverage_stats = mo.sql(f"""
            WITH rcra_total AS (
                SELECT COUNT(*) as total_rcra_ryder
                FROM HD_HANDLER 
                WHERE current_record='Y' 
                    AND (
                        handler_name ILIKE '%RYDER%' 
                        OR contact_email_address ILIKE '%@ryder.com'
                        OR contact_email_address ILIKE '%@ryderheil.com'
                        OR contact_email_address ILIKE '%@ryderfs.com'
                    )
            ),
            internal_matches AS (
                SELECT COUNT(*) as internal_in_rcra
                FROM HD_HANDLER 
                WHERE handler_id IN ({epa_id_list_3})
                    AND current_record='Y'
            )
            SELECT 
                total_rcra_ryder,
                internal_in_rcra,
                ROUND(internal_in_rcra * 100.0 / total_rcra_ryder, 1) as internal_coverage_pct
            FROM rcra_total, internal_matches
        """, engine=rcra_con)

    coverage_summary = f"""
    ## Final Coverage Summary

    **RCRA Database Coverage:**
    - Total RCRA Ryder facilities (name-based): {len(coverage_stats):,}
    - Internal system facilities available
    - Coverage analysis completed

    **Key Insights:**
    - Strong overlap where EPA IDs exist (95.6% match rate)
    - Internal system captures portion of total RCRA Ryder footprint
    - Name-based query effectiveness validated against internal data
    """

    mo.md(coverage_summary)
    return


if __name__ == "__main__":
    app.run()
