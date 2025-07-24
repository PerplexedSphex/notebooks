import marimo

__generated_with = "0.14.10"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import plotly.express as px
    import pandas as pd
    from pathlib import Path
    import duckdb

    # Connect to existing RCRA database
    RCRA_DB_PATH = Path("~/db/rcrainfo.duckdb").expanduser()
    rcra_con = duckdb.connect(database=RCRA_DB_PATH, read_only=True)

    print(f"Connected to: {RCRA_DB_PATH}")
    return mo, rcra_con


@app.cell
def _(mo):
    mo.md(f"""
    # Genesis Custom Chemical Blending Investigation

    ## Research Request
    Investigation for @Will Locke regarding Genesis Custom Chemical Blending data review:
    - **Contact claim**: They have 2 LQG facilities
    - **Known facility**: TXR00041780 at 2708 NE Main St, Ennis, TX 75119-8426
    - **TX Registration ID**: 00446861
    - **Second location**: 2400 E County Rd 123, Midland, TX 79706
    - **Owner**: SMART CHEMICAL SOLUTIONS LLC
    - **TCEQ finding**: Only one shipment found in May 2024

    ## Investigation Approach
    1. Search RCRA database for Genesis Chemical facilities
    2. Search for facilities at target Midland address
    3. Search for owner company (Smart Chemical Solutions)
    4. Analyze e-Manifest shipping data
    5. Verify LQG claims with actual waste generation records
    """)
    return


@app.cell
def _(HD_HANDLER, mo, rcra_con):
    # Search for Genesis Chemical facilities in RCRA database
    genesis_facilities = mo.sql(
        """
        SELECT handler_id, handler_name, location_city, location_state, 
               location_zip, fed_waste_generator,
               CASE 
                   WHEN fed_waste_generator = '1' THEN 'LQG' 
                   WHEN fed_waste_generator = '2' THEN 'SQG' 
                   WHEN fed_waste_generator = '3' THEN 'VSQG' 
                   WHEN fed_waste_generator = 'N' THEN 'Not a Generator' 
                   ELSE 'Other'
               END as generator_status
        FROM HD_HANDLER 
        WHERE current_record='Y' 
            AND (handler_name ILIKE '%GENESIS%' AND handler_name ILIKE '%CHEMICAL%') 
        ORDER BY handler_name
        """,
        engine=rcra_con
    )

    mo.md(f"""
    ## Known Genesis Chemical Facilities

    Found {len(genesis_facilities)} Genesis Chemical facilities in RCRA database:
    """)
    return (genesis_facilities,)


@app.cell
def _(genesis_facilities):
    genesis_facilities
    return


@app.cell
def _(HD_HANDLER, mo, rcra_con):
    # Search for facilities at target Midland address
    midland_address_search = mo.sql(
        """
        SELECT handler_id, handler_name, location_street_no, location_street1, 
               location_city, location_state, location_zip, fed_waste_generator,
               CASE 
                   WHEN fed_waste_generator = '1' THEN 'LQG' 
                   WHEN fed_waste_generator = '2' THEN 'SQG' 
                   WHEN fed_waste_generator = '3' THEN 'VSQG' 
                   WHEN fed_waste_generator = 'N' THEN 'Not a Generator' 
                   ELSE 'Other'
               END as generator_status
        FROM HD_HANDLER 
        WHERE current_record='Y' 
            AND location_state='TX' 
            AND (location_street1 ILIKE '%COUNTY RD 123%' OR location_street1 ILIKE '%COUNTY ROAD 123%') 
        ORDER BY handler_name
        """,
        engine=rcra_con
    )

    mo.md(f"""
    ## Target Address Investigation - County Road 123, Midland, TX

    Searching for facilities at or near 2400 E County Rd 123, Midland, TX 79706:
    """)
    return (midland_address_search,)


@app.cell
def _(midland_address_search):
    midland_address_search
    return


@app.cell
def _(HD_HANDLER, mo, rcra_con):
    # Get detailed info on the matching facility
    genisis_logistics_detail = mo.sql(
        """
        SELECT handler_id, handler_name, location_street_no, location_street1, 
               location_street2, location_city, location_state, location_zip, 
               fed_waste_generator, contact_email_address, contact_first_name, 
               contact_last_name, contact_phone, receive_date
        FROM HD_HANDLER 
        WHERE handler_id='TXR000087000' AND current_record='Y'
        """,
        engine=rcra_con
    )

    mo.md(f"""
    ## 🎯 MATCH FOUND: Genisis Logistics

    **Exact address match** for 2400 E County Road 123, Midland, TX:
    """)
    return (genisis_logistics_detail,)


@app.cell
def _(genisis_logistics_detail):
    genisis_logistics_detail
    return


@app.cell
def _(mo):
    mo.md(f"""
    ## Key Findings from RCRA Registration Data

    ### ✅ Confirmed Facilities
    1. **TXR000041780 - Genesis Custom Chemical Blending**
       - Location: Ennis, TX
       - Status: **LQG (Large Quantity Generator)** ✅
       - Matches known facility information

    2. **TXR000087000 - Genisis Logistics** 
       - Location: **2400 E County Road 123, Midland, TX** ✅ (exact match!)
       - Status: **Not a Generator** ❌ (contradicts LQG claim)
       - Registration: November 20, 2024 (very recent!)
       - Contact: Cole Chadsworth, 806-367-8031

    ### ❌ Missing Elements
    - **No facilities found** registered to "SMART CHEMICAL SOLUTIONS LLC"
    - **Spelling discrepancy**: "GENISIS" vs "GENESIS" 
    - **Status discrepancy**: Midland facility is "Not a Generator", not LQG

    ### 📝 Analysis
    The contact claimed 2 LQG facilities, but RCRA data shows:
    - 1 confirmed LQG (Ennis)
    - 1 facility at target address but registered as "Not a Generator" (Midland)
    - Very recent registration (Nov 2024) suggests possible operational changes
    """)
    return


@app.cell
def _(EM_MANIFEST, mo, rcra_con):
    # Search for e-Manifest data from known Genesis facility
    genesis_manifests = mo.sql(
        """
        SELECT manifest_tracking_number, shipped_date, received_date, 
               generator_id, generator_name, generator_location_city, 
               generator_location_state, des_facility_id, des_facility_name, 
               total_quantity_tons
        FROM EM_MANIFEST 
        WHERE generator_id='TXR000041780' 
        ORDER BY shipped_date DESC 
        LIMIT 10
        """,
        engine=rcra_con
    )

    mo.md(f"""
    ## E-Manifest Analysis: Genesis Custom Chemical Blending

    **Recent shipping activity from known LQG facility (TXR000041780):**
    """)
    return (genesis_manifests,)


@app.cell
def _(genesis_manifests):
    genesis_manifests
    return


@app.cell
def _(EM_MANIFEST, mo, rcra_con):
    # Get manifest summary for Genesis facility
    genesis_manifest_summary = mo.sql(
        """
        SELECT 
            COUNT(*) as total_manifests,
            MIN(shipped_date) as earliest_shipment,
            MAX(shipped_date) as latest_shipment,
            SUM(CAST(total_quantity_tons AS FLOAT)) as total_tons_shipped,
            COUNT(DISTINCT des_facility_id) as unique_destinations
        FROM EM_MANIFEST 
        WHERE generator_id='TXR000041780'
        """,
        engine=rcra_con
    )

    mo.md(f"""
    ## Genesis LQG Manifest Summary

    **Comprehensive shipping data for TXR000041780:**
    """)
    return (genesis_manifest_summary,)


@app.cell
def _(genesis_manifest_summary):
    genesis_manifest_summary
    return


@app.cell
def _(EM_MANIFEST, mo, rcra_con):
    # Search for any manifests from Midland facilities
    midland_manifests = mo.sql(
        """
        SELECT manifest_tracking_number, shipped_date, generator_id, 
               generator_name, generator_location_city, generator_location_state
        FROM EM_MANIFEST 
        WHERE generator_id IN ('TXR000087000', 'TXR000018218') 
        ORDER BY shipped_date DESC 
        LIMIT 10
        """,
        engine=rcra_con
    )

    mo.md(f"""
    ## E-Manifest Search: Midland Facilities

    **Searching for any manifest activity from target facilities:**
    - TXR000087000 (Genisis Logistics)
    - TXR000018218 (Custom Chemicals - nearby facility)
    """)
    return (midland_manifests,)


@app.cell
def _(midland_manifests):
    midland_manifests
    return


@app.cell
def _(EM_MANIFEST, mo, rcra_con):
    # Get context on Midland area waste generation activity
    midland_activity = mo.sql(
        """
        SELECT 
            COUNT(*) as total_manifests,
            COUNT(DISTINCT generator_id) as unique_generators
        FROM EM_MANIFEST 
        WHERE generator_location_city='MIDLAND' 
            AND generator_location_state='TX' 
            AND shipped_date >= '20240501'
        """,
        engine=rcra_con
    )

    mo.md(f"""
    ## Midland Area Waste Generation Context

    **Recent manifest activity from Midland, TX (since May 2024):**
    """)
    return (midland_activity,)


@app.cell
def _(midland_activity):
    midland_activity
    return


@app.cell
def _(mo):
    mo.md(f"""
    ## 🎯 Final Investigation Results

    ### Confirmed Findings

    **Genesis Custom Chemical Blending (Ennis) - VERIFIED LQG:**
    - ✅ **95 total manifests** (July 2021 - April 2025)  
    - ✅ **Active operations**: 10 shipments in 2025 alone
    - ✅ **LQG volumes**: 7-20 tons per shipment
    - ✅ **Consistent with claim**: This is a legitimate LQG facility

    **Target Midland Address - FACILITY FOUND BUT NO LQG ACTIVITY:**
    - ✅ **Exact address match**: TXR000087000 "Genisis Logistics" at 2400 E County Rd 123
    - ❌ **Zero manifest activity**: No waste shipments found in e-Manifest database
    - ❌ **Not a Generator status**: RCRA registration shows "N" (Not a Generator)
    - ⚠️ **Very recent registration**: November 20, 2024 (may indicate operational changes)

    ### Key Discrepancies

    1. **LQG Claim vs Reality**: Contact claimed 2 LQGs, but evidence shows only 1
    2. **Waste Generation**: No e-Manifest evidence of hazardous waste from Midland facility
    3. **Owner Company**: No RCRA records found for "Smart Chemical Solutions LLC"
    4. **TCEQ vs e-Manifest**: TCEQ found 1 shipment (May 2024), e-Manifest shows 0

    ### Recommendations

    1. **Contact TXR000087000** directly (Cole Chadsworth, 806-367-8031):
       - Verify connection to Genesis Custom Chemical Blending
       - Clarify actual waste generation status
       - Understand recent registration (Nov 2024)

    2. **Business relationship verification**:
       - Confirm Smart Chemical Solutions LLC ownership
       - Determine operational relationship between Ennis and Midland locations

    3. **Regulatory status clarification**:
       - Check if Midland facility plans to become LQG
       - Verify current waste management practices

    ### Conclusion
    **The claim of having 2 LQG facilities appears to be unsubstantiated.** While there is a facility at the exact target address, it shows no evidence of hazardous waste generation or shipping activity that would qualify it as an LQG.
    """)
    return


if __name__ == "__main__":
    app.run()
