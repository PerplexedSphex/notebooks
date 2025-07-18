import marimo

__generated_with = "0.14.10"
app = marimo.App(width="columns", layout_file="layouts/gov_data.grid.json")


@app.cell(column=0)
def _():
    import marimo as mo
    import os, re, shutil, zipfile, requests, duckdb, logging
    from pathlib import Path
    from datetime import datetime, timedelta
    from tqdm.auto import tqdm
    return (
        Path,
        datetime,
        duckdb,
        logging,
        mo,
        re,
        requests,
        shutil,
        timedelta,
        tqdm,
        zipfile,
    )


@app.cell
def _(Path, logging):
    # ────────────────────────────────
    # Basic settings
    # ────────────────────────────────
    DATA_ROOT      = Path("~/data").expanduser()
    DB_ROOT      = Path("~/db").expanduser()
    RAW_DATA_DIR   = DATA_ROOT / "gov_raw"
    RCRA_DB_PATH   = DB_ROOT / "rcrainfo.duckdb"
    ECHO_DB_PATH   = DB_ROOT / "echo.duckdb"

    RCRA_DATASETS = {
        "hd":       "https://s3.amazonaws.com/rcrainfo-ftp/Production/CSV-{date}/Handler/HD.zip",
        "ce":       "https://s3.amazonaws.com/rcrainfo-ftp/Production/CSV-{date}/Compliance,%20Monitoring%20and%20Enforcement/CE.zip",
        "ca":       "https://s3.amazonaws.com/rcrainfo-ftp/Production/CSV-{date}/Corrective%20Action/CA.zip",
        "br":       "https://s3.amazonaws.com/rcrainfo-ftp/Production/CSV-{date}/Biennial%20Report/BR.zip",
        "em":       "https://s3.amazonaws.com/rcrainfo-ftp/Production/CSV-{date}/eManifest/EM.zip",
        "fa":       "https://s3.amazonaws.com/rcrainfo-ftp/Production/CSV-{date}/Financial%20Assurance/FA.zip",
        "pm":       "https://s3.amazonaws.com/rcrainfo-ftp/Production/CSV-{date}/Permitting/PM.zip"
    }

    ECHO_DATASETS = {
        "air":      "https://echo.epa.gov/files/echodownloads/ICIS-AIR_downloads.zip",
        "water":    "https://echo.epa.gov/files/echodownloads/npdes_downloads.zip",
        "rcra":     "https://echo.epa.gov/files/echodownloads/rcra_downloads.zip",
        "rcra_viol":"https://echo.epa.gov/files/echodownloads/pipeline_rcra_downloads.zip", 
        "frs":      "https://ordsext.epa.gov/FLA/www3/state_files/national_combined.zip"
    }

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    log = logging.getLogger("etl")
    return (
        DB_ROOT,
        ECHO_DATASETS,
        ECHO_DB_PATH,
        RAW_DATA_DIR,
        RCRA_DATASETS,
        RCRA_DB_PATH,
        log,
    )


@app.cell
def _(
    ECHO_DATASETS,
    ECHO_DB_PATH,
    Path,
    RAW_DATA_DIR,
    RCRA_DATASETS,
    RCRA_DB_PATH,
    datetime,
    duckdb,
    log,
    re,
    requests,
    shutil,
    timedelta,
    tqdm,
    zipfile,
):
    # ────────────────────────────────
    # Helpers
    # ────────────────────────────────
    def latest_monday_stamp() -> str:
        monday = datetime.now() - timedelta(days=datetime.now().weekday())
        return monday.strftime("%Y-%m-%dT03-00-00-0400")

    def download(url: str, to: Path, chunk=8192) -> Path:
        to.parent.mkdir(parents=True, exist_ok=True)
        r = requests.get(url, stream=True)
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        with tqdm(total=total, unit="B", unit_scale=True, desc=to.name) as bar, open(to, "wb") as f:
            for blk in r.iter_content(chunk):
                if blk:
                    f.write(blk); bar.update(len(blk))
        return to

    def unzip_recursive(zfile: Path, dest: Path):
        dest.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zfile) as z:
            z.extractall(dest)
        for p in dest.rglob("*.zip"):
            subdir = p.with_suffix('')
            unzip_recursive(p, subdir)
            p.unlink()  # remove nested zip

    def snake(name: str) -> str:
        name = name.lower()
        name = re.sub(r'[^a-z0-9]+', '_', name).strip('_')
        return re.sub(r'_+', '_', name)

    def load_csvs(con, csvs: list[Path], table: str, bar=None) -> int:
        if not csvs:
            return 0
        cols = con.execute(
            f"SELECT * FROM read_csv_auto('{csvs[0]}', ALL_VARCHAR=TRUE, header=True) LIMIT 0"
        ).fetchdf().columns
        rename = ", ".join(f'"{c}" AS {snake(c)}' for c in cols)
        union  = " UNION ALL ".join(
            f"SELECT {rename} FROM read_csv_auto('{f}', ALL_VARCHAR=TRUE, header=True)"
            for f in csvs
        )
        con.execute(f"CREATE OR REPLACE TABLE {table} AS {union}")
        if bar:
            n = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            bar.set_postfix_str(f"{table}  {n:,} rows")
            bar.update(len(csvs))
        return len(csvs)

    # ────────────────────────────────
    # RCRA ETL
    # ────────────────────────────────
    def run_etl(key: str, skip_download=False):
        if key not in RCRA_DATASETS:
            raise ValueError(f"Choose from {list(RCRA_DATASETS)}")

        date   = latest_monday_stamp()
        url    = RCRA_DATASETS[key].format(date=date)
        raw    = RAW_DATA_DIR / key / date
        zfile  = RAW_DATA_DIR / f"{key}.zip"

        log.info(f"[{key}] snapshot {date}")

        if not skip_download:
            download(url, zfile)
            if raw.exists(): shutil.rmtree(raw)
            unzip_recursive(zfile, raw)
            zfile.unlink()

        targets = sorted([d for d in raw.iterdir() if d.is_dir()]) or [raw]

        # compute total files once for the bar
        total_files = sum(len(list(d.glob("*.csv"))) for d in targets)

        with duckdb.connect(RCRA_DB_PATH) as con, tqdm(total=total_files, desc="Ingest", unit="file") as bar:
            for d in targets:
                csvs = list(d.glob("*.csv"))
                name = d.name if d.is_dir() else key
                load_csvs(con, csvs, name, bar)

        log.info(f"✅  ETL done for '{key}' — {len(targets)} tables ingested")

    # ────────────────────────────────
    # ECHO ETL
    # ────────────────────────────────
    def run_echo_etl(dataset: str, skip_download=False):
        if dataset not in ECHO_DATASETS:
            raise ValueError(f"Choose from {list(ECHO_DATASETS)}")

        url       = ECHO_DATASETS[dataset]
        data_dir  = RAW_DATA_DIR / dataset
        zip_path  = data_dir / f"{dataset}.zip"
        extract   = data_dir / "extracted"

        log.info(f"[{dataset}] ETL start")

        if not skip_download:
            download(url, zip_path)
        elif not zip_path.exists():
            raise FileNotFoundError(zip_path)

        if extract.exists(): shutil.rmtree(extract)
        unzip_recursive(zip_path, extract)

        csvs = list(extract.rglob("*.csv")) + list(extract.rglob("*.CSV"))
        log.info(f"📋 Found {len(csvs)} CSV files")

        total_files = len(csvs)

        with duckdb.connect(ECHO_DB_PATH) as con, tqdm(total=total_files, desc="Ingest", unit='file') as bar:
            for csv in csvs:
                table = f"{snake(csv.stem)}"
                load_csvs(con, [csv], table, bar)

        log.info(f"✅  ETL done – {len(csvs)} tables, DB → {ECHO_DB_PATH.resolve()}")
    return run_echo_etl, run_etl


@app.cell
def _(mo):
    dataset_selector = mo.ui.dropdown(
        options=list(['rcrainfo', 'echo+frs']),
        value='rcrainfo',
        label="Select Dataset to ETL"
    )

    run_etl_button = mo.ui.run_button(label="Run ETL")

    mo.hstack([dataset_selector, run_etl_button])

    return dataset_selector, run_etl_button


@app.cell
def _(
    ECHO_DATASETS,
    RCRA_DATASETS,
    dataset_selector,
    log,
    run_echo_etl,
    run_etl,
    run_etl_button,
):
    if run_etl_button.value:
        if dataset_selector.value == 'rcrainfo':
            # Assuming 'rcra_datasets' is a list of all RCRA dataset identifiers
            for dataset_name in RCRA_DATASETS.keys():
                run_etl(dataset_name)
        elif dataset_selector.value == 'echo+frs':
            # Assuming 'echo_databases' is a list of all ECHO database identifiers
            for dataset_name in ECHO_DATASETS.keys():
                run_echo_etl(dataset_name)
        else:
            # For any other specific dataset selection, run ETL for that dataset
            log.info(f"None - try again")
    return


@app.cell
def _(DB_ROOT, duckdb):
    rcra_con = duckdb.connect(database= DB_ROOT / "rcrainfo.duckdb")
    echo_con = duckdb.connect(database= DB_ROOT / "echo.duckdb")

    print("Tables in rcrainfo.duckdb:")
    print(rcra_con.execute("PRAGMA show_tables;").fetchdf())

    print("\nTables in echo.duckdb:")
    print(echo_con.execute("PRAGMA show_tables;").fetchdf())
    return echo_con, rcra_con


@app.cell(column=1)
def _():
    return


@app.cell
def _(echo_con, rcra_con):
    import pandas as pd

    def find_email_columns(connection, db_name):
        """Find all tables and columns that contain email addresses"""
    
        # Get all tables and their columns
        tables_query = """
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'main'
        ORDER BY table_name, column_name
        """
    
        tables_info = connection.execute(tables_query).fetchdf()
    
        email_results = []
    
        print(f"\n🔍 Searching for emails in {db_name}...")
    
        for _, row in tables_info.iterrows():
            table_name = row['table_name']
            column_name = row['column_name']
        
            # Check if column contains any '@' symbols
            try:
                count_query = f"""
                SELECT COUNT(*) as email_count
                FROM {table_name}
                WHERE {column_name} LIKE '%@%'
                AND {column_name} IS NOT NULL
                """
            
                result = connection.execute(count_query).fetchone()
                email_count = result[0] if result else 0
            
                if email_count > 0:
                    # Get some sample emails
                    sample_query = f"""
                    SELECT {column_name}
                    FROM {table_name}
                    WHERE {column_name} LIKE '%@%'
                    AND {column_name} IS NOT NULL
                    LIMIT 5
                    """
                    samples = connection.execute(sample_query).fetchdf()
                
                    email_results.append({
                        'database': db_name,
                        'table': table_name,
                        'column': column_name,
                        'email_count': email_count,
                        'samples': samples[column_name].tolist()
                    })
                
            except Exception as e:
                # Skip columns that can't be queried (e.g., binary columns)
                continue
    
        return email_results

    # Search both databases
    rcra_emails = find_email_columns(rcra_con, "rcrainfo")
    echo_emails = find_email_columns(echo_con, "echo")

    # Combine results
    all_email_results = rcra_emails + echo_emails

    # Convert to DataFrame and sort by email count descending
    email_df = pd.DataFrame(all_email_results)
    email_df = email_df.sort_values('email_count', ascending=False).reset_index(drop=True)
    email_df
    return (pd,)


@app.cell
def _(
    HD_CERTIFICATION,
    HD_HADDL_CONTACT,
    HD_HANDLER,
    HD_OWNER_OPERATOR,
    HD_REPORTING,
    mo,
    rcra_con,
):
    rcrainfo_domain_facilities = mo.sql(
        f"""
        with handler_contacts as (
            SELECT  handler_id
                ,activity_location
                ,source_type
                ,seq_number
                ,handler_name
                ,contact_email_address as email
            FROM "HD_HANDLER"
                where current_record='Y'
        ),
        cert_contacts as (
            SELECT b.handler_id
                ,b.activity_location
                ,b.source_type
                ,b.seq_number
                ,b.cert_email as email
            FROM handler_contacts a join "HD_CERTIFICATION" b
                on a.handler_id=b.handler_id
                    and a.activity_location=b.activity_location
                    and a.source_type=b.source_type
                    and a.seq_number=b.seq_number
        ),
        reporting_contacts as (
            SELECT b.handler_id
                ,b.activity_location
                ,b.source_type
                ,b.seq_number
                ,b.contact_email_address as email
            FROM handler_contacts a join "HD_REPORTING" b
                on a.handler_id=b.handler_id
                    and a.activity_location=b.activity_location
                    and a.source_type=b.source_type
                    and a.seq_number=b.seq_number
        ),
        oop_contacts as (
            SELECT b.handler_id
                ,b.activity_location
                ,b.source_type
                ,b.seq_number
                ,b.email as email
            FROM handler_contacts a join "HD_OWNER_OPERATOR" b
                on a.handler_id=b.handler_id
                    and a.activity_location=b.activity_location
                    and a.source_type=b.source_type
                    and a.seq_number=b.seq_number
        ),
        haddl_contacts as (
            SELECT b.handler_id
                ,b.activity_location
                ,b.source_type
                ,b.seq_number
                ,b.addl_contact_email_address as email
            FROM handler_contacts a join "HD_HADDL_CONTACT" b
                on a.handler_id=b.handler_id
                    and a.activity_location=b.activity_location
                    and a.source_type=b.source_type
                    and a.seq_number=b.seq_number
        ),
        contact_union as (
            select handler_id, email from handler_contacts
            union
            select handler_id, email from cert_contacts
            union
            select handler_id, email from reporting_contacts
            union
            select handler_id, email from oop_contacts
            union
            select handler_id, email from haddl_contacts
        )
        select distinct
            handler_id
            ,split_part(email,'@',2) as contact_domain
        from contact_union
        where contact_domain <> '' and contact_domain is not null and contact_domain <> '0.0'
        order by contact_domain


        -- rcrainfo	HD_HANDLER	contact_email_address
        -- echo	national_contact_file	email_address
        -- rcrainfo	HD_CERTIFICATION	cert_email
        -- rcrainfo	HD_REPORTING	contact_email_address
        -- rcrainfo	HD_OWNER_OPERATOR	email
        -- echo	national_organization_file	email_address
        -- rcrainfo	HD_HADDL_CONTACT	addl_contact_email_address
        """,
        engine=rcra_con
    )
    return


@app.cell
def _(echo_con, mo, national_contact_file, national_organization_file):
    echo_domain_facilities = mo.sql(
        f"""
        with contact_all as (
            SELECT registry_id, email_address FROM national_contact_file
            union
            SELECT registry_id, email_address FROM national_organization_file
        )
        select distinct registry_id
            ,split_part(email_address,'@',2) as contact_domain
        from contact_all
        where contact_domain <> '' and contact_domain is not null and contact_domain <> '0.0'
        order by contact_domain
        """,
        engine=echo_con
    )
    return (echo_domain_facilities,)


@app.cell(column=2)
def _(mo):
    prospect_domain = mo.ui.text(placeholder="Enter domain name", label="Prospect Domain")
    prospect_domain
    return (prospect_domain,)


@app.cell
def _(echo_con, echo_domain_facilities, mo, prospect_domain, tmp_ids):
    import polars as pl

    prospect = prospect_domain.value.upper()

    filtered = (
        echo_domain_facilities
            .filter(pl.col("contact_domain").str.to_uppercase() == prospect)
    )

    echo_con.register("tmp_ids", filtered.select("registry_id"))

    ids = mo.sql(
        f"""
        select * from tmp_ids
        """
        ,engine=echo_con
    )

    ids
    return


@app.cell
def _(duckdb, pd):
    # ──────────────────────────────────────────────────────────────────────────
    #  Notebook‑ready Enhanced ECHO Analysis  (full logic preserved)
    # ──────────────────────────────────────────────────────────────────────────
    from __future__ import annotations
    from datetime import date
    from functools import lru_cache
    from types import SimpleNamespace
    import numpy as np

    # ─── 1. SQL helper functions (same outputs, now id_table‑driven) ──────────
    def _air_q_eval(wb, we, idt="_ids"):  # FCE/PCE
        return f"""
        SELECT e.pgm_sys_id || '|' || e.activity_id || '|' || e.actual_end_date AS eval_pk,
               e.pgm_sys_id,
               CAST(f.registry_id AS VARCHAR) AS registry_id,
               e.activity_id,
               strftime(strptime(e.actual_end_date,'%m-%d-%Y'),'%m/%d/%Y') AS actual_end_date,
               strftime(strptime(e.actual_end_date,'%m-%d-%Y'),'%m/%d/%Y') AS actual_begin_date,
               e.state_epa_flag,
               EXTRACT(YEAR FROM strptime(e.actual_end_date,'%m-%d-%Y')) AS eval_year,
               e.activity_type_desc AS eval_type_desc,
               CASE WHEN e.comp_monitor_type_desc LIKE '%FCE%' THEN 'Y' ELSE 'N' END AS found_violation,
               strptime(e.actual_end_date,'%m-%d-%Y') AS actual_end_date_dt
        FROM icis_air_fces_pces e
        JOIN icis_air_facilities f USING (pgm_sys_id)
        WHERE strptime(e.actual_end_date,'%m-%d-%Y') BETWEEN '{wb}' AND '{we}'
          AND CAST(f.registry_id AS VARCHAR) IN (SELECT id FROM {idt})
        """

    def _air_q_viol(wb, we, idt="_ids"):
        return f"""
        SELECT v.pgm_sys_id || '|' || v.activity_id || '|' || v.earliest_frv_determ_date AS viol_pk,
               v.pgm_sys_id,
               v.activity_id AS air_violation_id,
               strftime(strptime(v.earliest_frv_determ_date,'%m-%d-%Y'),'%m/%d/%Y') AS rnc_detection_date,
               strftime(strptime(v.hpv_resolved_date,'%m-%d-%Y'),'%m/%d/%Y')       AS rnc_resolution_date,
               EXTRACT(YEAR FROM strptime(v.earliest_frv_determ_date,'%m-%d-%Y'))  AS viol_year,
               COALESCE(v.program_descs,'Air Program Violation') AS viol_short_desc,
               CASE WHEN v.hpv_resolved_date IS NOT NULL
                    THEN DATE_DIFF('day',
                           strptime(v.earliest_frv_determ_date,'%m-%d-%Y'),
                           strptime(v.hpv_resolved_date,'%m-%d-%Y')) END AS days_to_resolve,
               strptime(v.earliest_frv_determ_date,'%m-%d-%Y') AS rnc_detection_date_dt
        FROM icis_air_violation_history v
        JOIN icis_air_facilities f USING (pgm_sys_id)
        WHERE strptime(v.earliest_frv_determ_date,'%m-%d-%Y') BETWEEN '{wb}' AND '{we}'
          AND CAST(f.registry_id AS VARCHAR) IN (SELECT id FROM {idt})
        """

    def _air_q_enf(wb, we, idt="_ids"):
        return f"""
        WITH formal AS (
            SELECT fa.pgm_sys_id, CAST(f.registry_id AS VARCHAR) AS registry_id,
                   fa.activity_id, fa.enf_identifier,
                   fa.settlement_entered_date,
                   'Formal' AS action_type, fa.activity_type_desc, fa.enf_type_desc,
                   COALESCE(CAST(fa.penalty_amount AS DOUBLE),0.0) AS penalty_amt,
                   strptime(fa.settlement_entered_date,'%m/%d/%Y') AS settlement_entered_date_dt,
                   fa.pgm_sys_id || '|' || fa.activity_id || '|' || fa.settlement_entered_date AS enf_pk
            FROM icis_air_formal_actions fa
            JOIN icis_air_facilities f USING (pgm_sys_id)
            WHERE fa.settlement_entered_date IS NOT NULL
              AND strptime(fa.settlement_entered_date,'%m/%d/%Y') BETWEEN '{wb}' AND '{we}'
              AND CAST(f.registry_id AS VARCHAR) IN (SELECT id FROM {idt})
        ),
        informal AS (
            SELECT ia.pgm_sys_id, CAST(f.registry_id AS VARCHAR) AS registry_id,
                   ia.activity_id, ia.enf_identifier,
                   ia.achieved_date AS settlement_entered_date,
                   'Informal' AS action_type, ia.activity_type_desc, ia.enf_type_desc,
                   0.0 AS penalty_amt,
                   strptime(ia.achieved_date,'%m/%d/%Y') AS settlement_entered_date_dt,
                   ia.pgm_sys_id || '|' || ia.activity_id || '|' || ia.achieved_date AS enf_pk
            FROM icis_air_informal_actions ia
            JOIN icis_air_facilities f USING (pgm_sys_id)
            WHERE ia.achieved_date IS NOT NULL
              AND strptime(ia.achieved_date,'%m/%d/%Y') BETWEEN '{wb}' AND '{we}'
              AND CAST(f.registry_id AS VARCHAR) IN (SELECT id FROM {idt})
        )
        SELECT * FROM formal UNION ALL SELECT * FROM informal
        """

    def _air_q_permits(idt="_ids"):
        return f"""
        SELECT pgm_sys_id,
               CAST(registry_id AS VARCHAR) AS registry_id,
               facility_name, city, state AS state_code,
               air_operating_status_code AS permit_status_code,
               air_operating_status_desc AS permit_status_desc,
               '01/01/1970' AS effective_date, '12/31/2099' AS expiration_date,
               1 AS version_nmbr
        FROM icis_air_facilities
        WHERE CAST(registry_id AS VARCHAR) IN (SELECT id FROM {idt})
        """

    AIR = SimpleNamespace(
        q_eval=_air_q_eval,  q_viol=_air_q_viol,
        q_enf=_air_q_enf,    q_permits=_air_q_permits,
        id_col="pgm_sys_id", fac_col="registry_id"
    )

    def _np_q_eval(wb, we, idt="_ids"):
        return f"""
        SELECT DISTINCT ON (eval_pk)
               CONCAT_WS('|',i.npdes_id,i.activity_id,i.actual_begin_date) AS eval_pk,
               i.npdes_id, f.facility_uin,
               i.activity_id, i.actual_begin_date, i.actual_end_date,
               EXTRACT(YEAR FROM strptime(i.actual_begin_date,'%m/%d/%Y')) AS eval_year,
               i.comp_monitor_type_desc AS eval_type_desc,
               'N' AS found_violation
        FROM water_npdes_inspections i
        JOIN water_icis_facilities f USING (npdes_id)
        WHERE strptime(i.actual_begin_date,'%m/%d/%Y') BETWEEN '{wb}' AND '{we}'
          AND CAST(f.facility_uin AS VARCHAR) IN (SELECT id FROM {idt})
        """

    def _np_q_viol(wb, we, idt="_ids"):
        return f"""
        SELECT DISTINCT ON (viol_pk)
               CONCAT_WS('|',v.npdes_id,v.npdes_violation_id,v.rnc_detection_date) AS viol_pk,
               v.*, CASE WHEN v.rnc_resolution_date IS NOT NULL
                         THEN DATE_DIFF('day',
                              strptime(v.rnc_detection_date,'%m/%d/%Y'),
                              strptime(v.rnc_resolution_date,'%m/%d/%Y')) END AS days_to_resolve
        FROM (
            SELECT * FROM water_npdes_ps_violations
            UNION ALL
            SELECT * FROM water_npdes_cs_violations
            UNION ALL
            SELECT * FROM water_npdes_se_violations
        ) v
        JOIN water_icis_facilities f USING (npdes_id)
        WHERE strptime(v.rnc_detection_date,'%m/%d/%Y') BETWEEN '{wb}' AND '{we}'
          AND CAST(f.facility_uin AS VARCHAR) IN (SELECT id FROM {idt})
        """

    def _np_q_enf(wb, we, idt="_ids"):
        return f"""
        SELECT DISTINCT ON (enf_pk)
               CONCAT_WS('|',e.npdes_id,e.enf_identifier,e.settlement_entered_date) AS enf_pk,
               e.*, COALESCE(CAST(e.fed_penalty_assessed_amt AS DOUBLE),0.0) +
                    COALESCE(CAST(e.state_local_penalty_amt  AS DOUBLE),0.0) AS penalty_amt
        FROM water_npdes_formal_enforcement_actions e
        JOIN water_icis_facilities f USING (npdes_id)
        WHERE strptime(e.settlement_entered_date,'%m/%d/%Y') BETWEEN '{wb}' AND '{we}'
          AND CAST(f.facility_uin AS VARCHAR) IN (SELECT id FROM {idt})
        """

    def _np_q_permits(idt="_ids"):
        return f"""
        SELECT p.external_permit_nmbr AS npdes_id,
               f.facility_uin, f.facility_name, f.city, f.state_code,
               p.effective_date, p.expiration_date,
               p.permit_status_code, p.version_nmbr
        FROM water_icis_permits p
        JOIN water_icis_facilities f ON p.external_permit_nmbr = f.npdes_id
        WHERE CAST(f.facility_uin AS VARCHAR) IN (SELECT id FROM {idt})
        """

    NPDES = SimpleNamespace(
        q_eval=_np_q_eval,   q_viol=_np_q_viol,
        q_enf=_np_q_enf,     q_permits=_np_q_permits,
        id_col="npdes_id",   fac_col="facility_uin"
    )

    # helper for date slicing
    def _slice(df,col,start,end):
        if df.empty: return df
        _df=df.copy()
        _df[f"{col}_dt"]=pd.to_datetime(_df[col],format="%m/%d/%Y",errors="coerce")
        return _df.query(f"{col}_dt>=@start and {col}_dt<=@end")

    # ─── 2. Driver (FULL Amazon‑style logic, minus Amazon) ────────────────────
    def run_echo_analysis(echo_con: duckdb.DuckDBPyConnection,
                          dataset:str="air",
                          years_lookback:int=5) -> dict[str,pd.DataFrame]:
        mod = AIR if dataset.lower()=="air" else NPDES
        today, wb = date.today(), date(date.today().year-years_lookback,1,1)

        # provide tmp_ids as view
        echo_con.execute("CREATE OR REPLACE TEMP VIEW _ids AS "
                         "SELECT CAST(registry_id AS VARCHAR) AS id FROM tmp_ids")

        evals = echo_con.execute(mod.q_eval(wb,today)).fetchdf()
        viols = echo_con.execute(mod.q_viol(wb,today)).fetchdf()
        enfs  = echo_con.execute(mod.q_enf(wb,today)).fetchdf()
        permits = echo_con.execute(mod.q_permits()).fetchdf()

        facilities = permits[[mod.id_col,mod.fac_col,"facility_name","city","state_code"]].drop_duplicates()

        # ── active facility helper (keeps STATUS logic for air) ───────────────
        if dataset.lower()=="air":
            status_hist = echo_con.execute(f"""
                SELECT ap.{mod.id_col}, CAST(f.{mod.fac_col} AS VARCHAR) AS {mod.fac_col},
                       ap.air_operating_status_code AS status_code,
                       COALESCE(ap.updated_date,ap.begin_date) AS status_date
                FROM icis_air_programs ap
                JOIN icis_air_facilities f USING ({mod.id_col})
                WHERE CAST(f.{mod.fac_col} AS VARCHAR) IN (SELECT id FROM _ids)
            """).fetchdf()
        else:
            status_hist = pd.DataFrame()

        @lru_cache
        def active_ids_asof(cutoff:date)->set[str]:
            if dataset.lower()=="water":
                p=permits.copy()
                p["effective_date"]=pd.to_datetime(p.effective_date,format="%m/%d/%Y",errors="coerce")
                p["expiration_date"]=pd.to_datetime(p.expiration_date,format="%m/%d/%Y",errors="coerce")
                cut=pd.Timestamp(cutoff)
                active=p[(p.effective_date<=cut)&((p.expiration_date.isna())|(p.expiration_date>=cut))&(
                         p.permit_status_code.isin(["EFF","ADC"]))]
                if active.empty: return set()
                return set(active.groupby(mod.id_col).head(1)[mod.id_col])
            # air
            if status_hist.empty:
                return set(permits[mod.id_col])  # snapshot fallback
            h=status_hist.copy()
            h["status_date"]=pd.to_datetime(h.status_date,format="%m/%d/%Y",errors="coerce")
            h=h[h.status_date<=pd.Timestamp(cutoff)]
            if h.empty: return set()
            latest=h.loc[h.groupby(mod.id_col).status_date.idxmax()]
            return set(latest[latest.status_code.str.strip().isin(["OPR","SEA","TMP","CNS","PLN"])][mod.id_col])

        active_now=active_ids_asof(today)
        fac_count=len(active_now)

        # ── metric builder (kept 100 % of original math) ───────────────────────
        def _metrics(df_eval,df_viol,df_enf,facilities_active:int)->dict:
            d={}
            d["facilities"]=facilities_active
            d["evaluations"]=df_eval.eval_pk.nunique()
            d["facs_with_eval"]=df_eval[mod.id_col].nunique()
            d["evals_with_viol"]=df_eval.loc[df_eval.found_violation=="Y","eval_pk"].nunique()
            d["violations"]=df_viol.viol_pk.nunique()
            d["enforcement_actions"]=df_enf.enf_pk.nunique()
            d["total_penalties"]=round(df_enf.penalty_amt.sum(),2)
            d["open_violations"]=df_viol.days_to_resolve.isna().sum()
            d["viol_rate_%"]=round(100*d["evals_with_viol"]/d["evaluations"],1) if d["evaluations"] else 0.0
            d["eval_rate_%"]=round(100*d["facs_with_eval"]/d["facilities"],1) if d["facilities"] else 0.0
            d["avg_days_to_resolve"]=int(df_viol.days_to_resolve.dropna().mean()) if not df_viol.days_to_resolve.dropna().empty else None
            d["avg_evals_per_evald_fac"]=round(d["evaluations"]/d["facs_with_eval"],2) if d["facs_with_eval"] else 0.0
            d["avg_viol_per_hit_eval"]=round(d["violations"]/d["evals_with_viol"],2) if d["evals_with_viol"] else 0.0
            d["expected_violations"]=round(
                d["facilities"]*(d["eval_rate_%"]/100)*d["avg_evals_per_evald_fac"]*
                (d["viol_rate_%"]/100)*d["avg_viol_per_hit_eval"],2)
            # breakdowns
            if d["evaluations"]:
                ec=df_eval.groupby("eval_type_desc").agg(
                    count=("eval_pk","nunique"),
                    viol_count=("found_violation",lambda s:(s=="Y").sum())
                ).reset_index()
                ec["percent_of_evals"]=round(100*ec["count"]/d["evaluations"],1)
                ec["viol_rate_%"]=round(100*ec["viol_count"]/ec["count"],1)
                d["eval_type_breakdown"]=ec.drop(columns=["viol_count"]).to_dict("records")
            else:
                d["eval_type_breakdown"]=[]
            if d["violations"]:
                vc=df_viol.groupby("viol_short_desc",dropna=False).agg(
                    count=("viol_pk","nunique"),
                    avg_resolve_days=("days_to_resolve","mean")
                ).reset_index()
                vc["percent_of_violations"]=round(100*vc["count"]/d["violations"],1)
                vc["avg_resolve_days"]=vc["avg_resolve_days"].round(1)
                d["viol_short_breakdown"]=vc.to_dict("records")
            else:
                d["viol_short_breakdown"]=[]
            return d

        overall=pd.DataFrame([_metrics(evals,viols,enfs,fac_count)])

        # ── quarterly breakdowns (full logic) ─────────────────────────────────
        qrows=[]
        periods=pd.period_range(start=pd.Period(year=wb.year,quarter=1,freq='Q'),
                                end=pd.Period(today,freq='Q'),freq='Q')
        for p in periods:
            q_start,q_end=p.start_time.date(),min(p.end_time.date(),today)
            active_q=active_ids_asof(q_end)
            facs_q=len(active_q)

            e_q=_slice(evals,"actual_begin_date",q_start,q_end)
            v_q=_slice(viols,"rnc_detection_date",q_start,q_end)
            f_q=_slice(enfs ,"settlement_entered_date",q_start,q_end)

            if not facs_q and e_q.empty and v_q.empty and f_q.empty:
                continue
            qrows.append({"quarter":f"{p.year}Q{p.quarter}",**_metrics(e_q,v_q,f_q,facs_q)})
        quarter_df=pd.DataFrame(qrows)

        return dict(
            evals_df=evals, viols_df=viols, enfs_df=enfs,
            permits_df=permits, facilities_df=facilities,
            account_summary_df=overall, quarter_summary_df=quarter_df
        )



    # ─── Example --------------------------------------------------------------
    # echo_con = duckdb.connect("echo.duckdb", read_only=True)
    # results = run_echo_analysis(echo_con, dataset="air", years_lookback=5)
    # results["account_summary_df"]
    # results["quarter_summary_df"]

    return (run_echo_analysis,)


@app.cell
def _(echo_con, run_echo_analysis):
    results = run_echo_analysis(echo_con, dataset="air", years_lookback=5)
    results["account_summary_df"]
    # results["quarter_summary_df"]
    return


if __name__ == "__main__":
    app.run()
