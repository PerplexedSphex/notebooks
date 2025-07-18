import marimo

__generated_with = "0.14.10"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import duckdb
    import polars as pl
    from pathlib import Path

    DATABASE_URL = Path("~/db/rcrainfo.duckdb").expanduser()
    rcrainfo= duckdb.connect(DATABASE_URL, read_only=True)

    def get_table_schema(table_name, engine):
        """Fetches the schema of a table using PRAGMA table_info and returns a Polars DataFrame."""
        query = f"PRAGMA table_info('{table_name}')"
        schema_df = mo.sql(query, engine=engine)
        schema_df_pl = pl.DataFrame(schema_df)
        schema_df_pl = schema_df_pl.with_columns(
            pl.lit(table_name).alias("table_name")
        )
        return schema_df_pl

    # Fetch schemas for both tables
    hd_handler_schema = get_table_schema('hd_handler', rcrainfo)
    hd_naics_schema = get_table_schema('hd_naics', rcrainfo)

    # Concatenate the schemas into a single DataFrame
    table_schemas = pl.concat([hd_handler_schema, hd_naics_schema])

    table_schemas
    return mo, pl, rcrainfo


@app.cell
def _(rcrainfo):
    def map_generator_status(status: str) -> str:
        """
        Maps the generator status codes to their corresponding descriptions.

        Args:
            status (str): The generator status code ('1', '2', '3', or 'N').

        Returns:
            str: The corresponding description ('LQG', 'SQG', 'VSQG', or 'Non-reporter').
                 Returns 'Unknown' if the input is not one of the expected codes.
        """
        if status == "1":
            return "LQG"
        elif status == "2" or status == "3":
            return "SQG"
        elif status == "N":
            return "Non-reporter"
        else:
            return "Unknown"

    # Register the Python function as a SQL function in DuckDB
    rcrainfo.create_function("map_generator_status", map_generator_status, return_type='VARCHAR')
    return


@app.cell
def _(hd_handler, hd_naics, mo, rcrainfo):
    joined_df = mo.sql(
        f"""
        SELECT
            h.handler_id,
            h.activity_location,
            h.source_type,
            h.seq_number,
            h.handler_name,
            h.receive_date,
            h.location_street_no,
            h.location_street1,
            h.location_street2,
            h.location_city,
            h.location_state,
            h.location_zip,
            h.fed_waste_generator,
            h.state_waste_generator,
            map_generator_status(h.fed_waste_generator) AS fed_waste_generator_desc,
            h.contact_first_name,
            h.contact_middle_initial,
            h.contact_last_name,
            h.contact_phone,
            h.contact_email_address,
            p.naics_code AS primary_naics
        FROM
            hd_handler h
        LEFT JOIN
            hd_naics p ON h.handler_id = p.handler_id
            AND h.activity_location = p.activity_location
            AND h.source_type = p.source_type
            AND h.seq_number = p.seq_number
            AND p.naics_seq = '1'
        WHERE h.current_record = 'Y'
        """,
        engine=rcrainfo
    )
    joined_df
    return (joined_df,)


@app.cell
def _(HD_LU_NAICS, joined_df, mo, rcrainfo):
    industry_handlers = mo.sql(
        f"""
        SELECT
            j.*,
            naics2.naics_desc AS naics_2_digit_desc,
            naics3.naics_desc AS naics_3_digit_desc,
            naics_full.naics_desc AS naics_full_desc
        FROM
            joined_df AS j
        LEFT JOIN
            HD_LU_NAICS AS naics2
            ON SUBSTRING(j.primary_naics, 1, 2) = naics2.naics_code
        LEFT JOIN
            HD_LU_NAICS AS naics3
            ON SUBSTRING(j.primary_naics, 1, 3) = naics3.naics_code
        LEFT JOIN
            HD_LU_NAICS AS naics_full
            ON j.primary_naics = naics_full.naics_code
        """,
        engine=rcrainfo
    )
    return (industry_handlers,)


@app.cell
def _(industry_handlers, mo, rcrainfo):
    _df = mo.sql(
        f"""
        SELECT
            naics_3_digit_desc,
            COUNT(DISTINCT CASE WHEN fed_waste_generator_desc = 'LQG' THEN handler_id END) AS LQG,
            COUNT(DISTINCT CASE WHEN fed_waste_generator_desc = 'SQG' THEN handler_id END) AS SQG,
            COUNT(DISTINCT CASE WHEN fed_waste_generator_desc = 'Non-reporter' THEN handler_id END) AS Non_reporter,
            COUNT(DISTINCT CASE WHEN fed_waste_generator_desc = 'Unknown' THEN handler_id END) AS Unknown,
            COUNT(DISTINCT handler_id) AS Total
        FROM
            industry_handlers
        GROUP BY
            naics_3_digit_desc
        ORDER BY
            Total DESC
        """,
        engine=rcrainfo
    )
    return


@app.cell
def _(industry_handlers, mo, pl):
    import altair as alt


    _df = mo.sql(
        f"""
        SELECT
            location_state,
            fed_waste_generator_desc,
            COUNT(DISTINCT handler_id) AS distinct_handler_count
        FROM
            industry_handlers
        WHERE
            naics_2_digit_desc IS NULL OR naics_2_digit_desc = ''
        GROUP BY
            location_state,
            fed_waste_generator_desc
        ORDER BY distinct_handler_count DESC
        """
    )

    # Ensure the DataFrame is sorted by distinct_handler_count in descending order
    # before calculating cumulative sums. This is crucial for a monotonic CDF.
    _df = _df.sort('distinct_handler_count', descending=True)

    # Calculate cumulative sum and percentage using Polars expressions
    _df = _df.with_columns([
        pl.col('distinct_handler_count').cum_sum().alias('cumulative_count'),
        (pl.col('distinct_handler_count').cum_sum() / pl.col('distinct_handler_count').sum()).mul(100).alias('cumulative_percentage')
    ])

    # Create a combined category for the x-axis labels
    _df = _df.with_columns(
        (pl.col('location_state') + ' - ' + pl.col('fed_waste_generator_desc')).alias('category_label')
    )

    # Create the Altair chart
    # Base chart with shared X-axis.
    # Crucially, sort the X-axis by 'distinct_handler_count' to ensure monotonic decrease
    # for both the bars and the cumulative line.
    base = alt.Chart(_df).encode(
        x=alt.X('category_label',
                title='Location State - Fed Waste Generator Description',
                axis=alt.Axis(labelAngle=45),
                sort=alt.EncodingSortField(field="distinct_handler_count", op="sum", order="descending"))
    )

    # Bar chart for distinct handler count
    bar_chart = base.mark_bar(color='skyblue').encode(
        y=alt.Y('distinct_handler_count', title='Distinct Handler Count'),
        tooltip=[
            alt.Tooltip('category_label', title='Category'),
            alt.Tooltip('distinct_handler_count', title='Count', format=',')
        ]
    )

    # Line chart for cumulative percentage
    line_chart = base.mark_line(point=True, color='red').encode(
        y=alt.Y('cumulative_percentage', title='Cumulative Percentage (%)'),
        tooltip=[
            alt.Tooltip('category_label', title='Category'),
            alt.Tooltip('cumulative_percentage', title='Cumulative %', format='.2f')
        ]
    )

    # Combine charts using layer and resolve scales for independent Y-axes
    # This creates two separate Y-axes, one on the left and one on the right.
    chart = alt.layer(bar_chart, line_chart).resolve_scale(
        y='independent'
    ).properties(
        title='Distinct Handler Count and Cumulative Percentage by State and Waste Generator Description'
    ).interactive() # Make the chart interactive for zooming and panning

    # Return the chart object directly as per Altair rule
    chart
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
