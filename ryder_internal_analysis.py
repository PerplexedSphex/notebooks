#!/usr/bin/env python3
"""
Ryder Internal Data Analysis
Compare internal system data with RCRA database
"""

import pandas as pd
import duckdb
from pathlib import Path

# Load internal Ryder data
ryder_facs = pd.read_csv("/Users/sam/global-context-buffer/ryder-facs.csv")
ryder_ids = pd.read_csv("/Users/sam/global-context-buffer/ryder-fac-ids.csv")

print("=== INTERNAL RYDER DATA OVERVIEW ===")
print(f"Facilities: {len(ryder_facs):,}")
print(f"EPA IDs: {len(ryder_ids[ryder_ids['Type'] == 'EPA']):,}")
print(f"Total ID records: {len(ryder_ids):,}")

# Connect to RCRA database
RCRA_DB_PATH = Path("~/db/rcrainfo.duckdb").expanduser()
rcra_con = duckdb.connect(database=RCRA_DB_PATH, read_only=True)

print("\n=== BUSINESS UNIT BREAKDOWN (INTERNAL) ===")
bu_counts = ryder_facs['Business Unit'].value_counts()
print(bu_counts)

print("\n=== FACILITY STATUS (INTERNAL) ===")
status_counts = ryder_facs['Status'].value_counts()
print(status_counts)

# Extract EPA IDs
epa_ids = ryder_ids[ryder_ids['Type'] == 'EPA']['Value'].tolist()
print(f"\n=== EPA ID OVERLAP ANALYSIS ===")
print(f"Internal EPA IDs: {len(epa_ids)}")

# Check overlap with RCRA database
if epa_ids:
    # Build IN clause for SQL query
    epa_id_list = "'" + "','".join(epa_ids) + "'"
    
    overlap_query = f"""
    SELECT COUNT(*) as matches
    FROM HD_HANDLER 
    WHERE handler_id IN ({epa_id_list})
        AND current_record='Y'
    """
    
    matches = rcra_con.execute(overlap_query).fetchone()[0]
    print(f"RCRA database matches: {matches}")
    print(f"Coverage: {matches/len(epa_ids)*100:.1f}%")
    
    # Get details of matched facilities
    detail_query = f"""
    SELECT handler_id, handler_name, location_city, location_state,
           fed_waste_generator,
           CASE 
               WHEN fed_waste_generator = '1' THEN 'LQG' 
               WHEN fed_waste_generator = '2' THEN 'SQG' 
               WHEN fed_waste_generator = '3' THEN 'VSQG' 
               WHEN fed_waste_generator = 'N' THEN 'Not a Generator' 
               ELSE 'Other'
           END as generator_status
    FROM HD_HANDLER 
    WHERE handler_id IN ({epa_id_list})
        AND current_record='Y'
    ORDER BY handler_name
    """
    
    matched_facs = rcra_con.execute(detail_query).fetchdf()
    print(f"\n=== MATCHED FACILITIES DETAILS ===")
    print(matched_facs.to_string())
    
    print(f"\n=== GENERATOR STATUS BREAKDOWN (MATCHED) ===")
    gen_status = matched_facs['generator_status'].value_counts()
    print(gen_status)

# Check for unmatched EPA IDs
unmatched_query = f"""
SELECT handler_id, handler_name, location_city, location_state
FROM HD_HANDLER 
WHERE handler_id NOT IN ({epa_id_list})
    AND current_record='Y'
    AND (
        handler_name ILIKE '%RYDER%' 
        OR contact_email_address ILIKE '%@ryder.com'
        OR contact_email_address ILIKE '%@ryderheil.com'
        OR contact_email_address ILIKE '%@ryderfs.com'
    )
LIMIT 10
"""

# Get total RCRA counts
total_rcra_query = """
SELECT COUNT(*) FROM HD_HANDLER 
WHERE current_record='Y' 
    AND (
        handler_name ILIKE '%RYDER%' 
        OR contact_email_address ILIKE '%@ryder.com'
        OR contact_email_address ILIKE '%@ryderheil.com'
        OR contact_email_address ILIKE '%@ryderfs.com'
    )
"""

total_rcra = rcra_con.execute(total_rcra_query).fetchone()[0]
unmatched_facs = rcra_con.execute(unmatched_query).fetchdf()

print(f"\n=== RCRA FACILITIES NOT IN INTERNAL SYSTEM (Sample) ===")
print(f"Total RCRA Ryder facilities: {total_rcra}")
print(f"Matched with internal: {matches}")
print(f"RCRA-only facilities: {total_rcra - matches}")
print(f"Internal coverage: {matches/total_rcra*100:.1f}%")
print("\nSample unmatched facilities:")
print(unmatched_facs.to_string())

rcra_con.close()