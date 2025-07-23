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


if __name__ == "__main__":
    app.run()
