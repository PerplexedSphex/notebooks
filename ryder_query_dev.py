#!/usr/bin/env python3
"""
Ryder Account Query Development Script
Iterative development tool for building RCRA database queries
"""

import duckdb
from pathlib import Path
import pandas as pd

# Database connection
RCRA_DB_PATH = Path("~/db/rcrainfo.duckdb").expanduser()

def get_connection():
    """Get read-only connection to RCRA database"""
    return duckdb.connect(database=RCRA_DB_PATH, read_only=True)

def run_query(query, description="Query"):
    """Execute query and display results"""
    print(f"\n=== {description} ===")
    print(f"Query: {query}")
    print("-" * 50)
    
    con = get_connection()
    try:
        result = con.execute(query).fetchdf()
        print(result.to_string())
        print(f"\nRows returned: {len(result)}")
        return result
    except Exception as e:
        print(f"Error: {e}")
        return None
    finally:
        con.close()

def explore_patterns(pattern_field, pattern_value, limit=20):
    """Explore naming patterns in the data"""
    query = f"""
    SELECT {pattern_field}, COUNT(*) as cnt 
    FROM HD_HANDLER 
    WHERE current_record='Y' AND {pattern_field} ILIKE '%{pattern_value}%'
    GROUP BY {pattern_field} 
    ORDER BY cnt DESC 
    LIMIT {limit}
    """
    return run_query(query, f"Pattern exploration: {pattern_field} LIKE '%{pattern_value}%'")

def test_where_clause(where_clause):
    """Test a WHERE clause and show summary"""
    query = f"""
    SELECT COUNT(*) as total_matches
    FROM HD_HANDLER 
    WHERE current_record='Y' AND ({where_clause})
    """
    return run_query(query, f"WHERE clause test: {where_clause}")

def show_sample_data(where_clause, limit=10):
    """Show sample records matching WHERE clause"""
    query = f"""
    SELECT handler_id, handler_name, contact_email_address, 
           fed_waste_generator, location_city, location_state
    FROM HD_HANDLER 
    WHERE current_record='Y' AND ({where_clause})
    LIMIT {limit}
    """
    return run_query(query, f"Sample data for: {where_clause}")

if __name__ == "__main__":
    print("Ryder Query Development Tool")
    print("=" * 40)
    
    # Initial exploration
    explore_patterns("handler_name", "RYDER", 25)
    explore_patterns("contact_email_address", "ryder", 15)
    
    print("\n" + "=" * 60)
    print("WHERE CLAUSE DEVELOPMENT")
    print("=" * 60)
    
    # Test basic patterns
    test_where_clause("handler_name ILIKE '%RYDER%'")
    test_where_clause("contact_email_address ILIKE '%@ryder.com'")
    
    # Combined basic clause
    where_basic = "handler_name ILIKE '%RYDER%' OR contact_email_address ILIKE '%@ryder.com'"
    test_where_clause(where_basic)
    show_sample_data(where_basic, 15)
    
    print("\n" + "=" * 60)
    print("EDGE CASE EXPLORATION")
    print("=" * 60)
    
    # Check for additional email domains
    run_query("""
        SELECT contact_email_address, COUNT(*) as cnt 
        FROM HD_HANDLER 
        WHERE current_record='Y' 
            AND contact_email_address ILIKE '%ryder%' 
            AND contact_email_address NOT ILIKE '%@ryder.com'
        GROUP BY contact_email_address 
        ORDER BY cnt DESC 
        LIMIT 10
    """, "Non-@ryder.com email domains")
    
    # Check for false positives (people with Ryder as last name)
    run_query("""
        SELECT handler_name, contact_email_address, COUNT(*) as cnt
        FROM HD_HANDLER 
        WHERE current_record='Y' 
            AND contact_email_address ILIKE '%ryder%'
            AND contact_email_address NOT ILIKE '%@ryder.com'
            AND contact_email_address NOT ILIKE '%@ryderheil.com'
        GROUP BY handler_name, contact_email_address
        ORDER BY cnt DESC
    """, "Potential false positives")
    
    # Test refined WHERE clause - better approach
    where_refined_v2 = """
        (
            handler_name ILIKE '%RYDER%' 
            OR contact_email_address ILIKE '%@ryder.com'
            OR contact_email_address ILIKE '%@ryderheil.com'
            OR contact_email_address ILIKE '%@ryderfs.com'
            OR contact_email_address ILIKE '%@ryderfes.com'
        )
        AND (
            contact_email_address IS NULL 
            OR contact_email_address NOT ILIKE '%@bxp.com'
        )
        AND (
            contact_email_address IS NULL 
            OR contact_email_address NOT ILIKE '%@microsoft.com'
        )
    """
    test_where_clause(where_refined_v2)
    show_sample_data(where_refined_v2, 10)
    
    print("\n" + "=" * 60)
    print("FINAL WHERE CLAUSE VALIDATION")
    print("=" * 60)
    
    # Final comprehensive WHERE clause
    where_final = """
        (
            handler_name ILIKE '%RYDER%' 
            OR contact_email_address ILIKE '%@ryder.com'
            OR contact_email_address ILIKE '%@ryderheil.com'
            OR contact_email_address ILIKE '%@ryderfs.com'
        )
    """
    
    test_where_clause(where_final)
    
    # Check what we might be missing
    run_query(f"""
        SELECT handler_name, contact_email_address, COUNT(*) as cnt
        FROM HD_HANDLER 
        WHERE current_record='Y' 
            AND contact_email_address ILIKE '%ryder%'
            AND NOT ({where_final})
        GROUP BY handler_name, contact_email_address
        ORDER BY cnt DESC
        LIMIT 10
    """, "Records excluded by final WHERE clause")
    
    print("\n" + "=" * 60)
    print("BUSINESS UNIT CATEGORIZATION DEVELOPMENT")
    print("=" * 60)
    
    # Analyze business unit patterns
    run_query(f"""
        SELECT 
            CASE 
                WHEN handler_name ILIKE '%TRUCK RENTAL%' THEN 'Truck Rental'
                WHEN handler_name ILIKE '%TRANSPORTATION SERVICES%' OR handler_name ILIKE '%TRANSPORTATION SERVICE%' THEN 'Transportation Services'
                WHEN handler_name ILIKE '%TRANSPORTATION%' AND handler_name NOT ILIKE '%TRUCK%' THEN 'Transportation Other'
                WHEN handler_name ILIKE '%INTEGRATED LOGISTICS%' THEN 'Integrated Logistics'
                WHEN handler_name ILIKE '%STUDENT TRANSPORTATION%' THEN 'Student Transportation'
                WHEN handler_name ILIKE '%LOGISTICS%' THEN 'Logistics Other'
                ELSE 'Other'
            END as business_unit_draft,
            COUNT(*) as cnt
        FROM HD_HANDLER 
        WHERE current_record='Y' AND ({where_final})
        GROUP BY 1
        ORDER BY cnt DESC
    """, "Initial business unit categorization")
    
    # Look at specific patterns that might need refinement
    run_query(f"""
        SELECT handler_name, COUNT(*) as cnt
        FROM HD_HANDLER 
        WHERE current_record='Y' 
            AND ({where_final})
            AND handler_name NOT ILIKE '%TRUCK RENTAL%'
            AND handler_name NOT ILIKE '%TRANSPORTATION SERVICES%'
            AND handler_name NOT ILIKE '%TRANSPORTATION SERVICE%'
            AND handler_name NOT ILIKE '%INTEGRATED LOGISTICS%'
            AND handler_name NOT ILIKE '%STUDENT TRANSPORTATION%'
        GROUP BY handler_name
        ORDER BY cnt DESC
        LIMIT 20
    """, "Uncategorized patterns needing attention")
    
    print("\n" + "=" * 60)
    print("REFINED BUSINESS UNIT CATEGORIZATION")
    print("=" * 60)
    
    # Refined business unit logic
    run_query(f"""
        SELECT 
            CASE 
                -- Truck rental is the largest segment
                WHEN handler_name ILIKE '%TRUCK RENTAL%' THEN 'Ryder Truck Rental'
                -- Transportation services variations
                WHEN handler_name ILIKE '%TRANSPORTATION SERVICES%' OR handler_name ILIKE '%TRANSPORTATION SERVICE%' THEN 'Ryder Transportation Services'
                WHEN handler_name ILIKE '%TRANSPORTATION SVCS%' OR handler_name ILIKE '%TRANSPORTATION SVC%' THEN 'Ryder Transportation Services'
                -- Integrated logistics
                WHEN handler_name ILIKE '%INTEGRATED LOGISTICS%' THEN 'Ryder Integrated Logistics'
                -- Student transportation
                WHEN handler_name ILIKE '%STUDENT TRANSPORTATION%' THEN 'Ryder Student Transportation'
                -- General transportation (catch remaining transportation)
                WHEN handler_name ILIKE '%TRANSPORTATION%' THEN 'Ryder Transportation Other'
                -- Systems/technology
                WHEN handler_name ILIKE '%SYSTEMS%' THEN 'Ryder Systems'
                -- Truck operations (non-rental)
                WHEN handler_name ILIKE '%TRUCK%' AND handler_name NOT ILIKE '%RENTAL%' THEN 'Ryder Truck Other'
                -- Logistics (general)
                WHEN handler_name ILIKE '%LOGISTICS%' THEN 'Ryder Logistics Other'
                -- Just 'RYDER' (corporate/other)
                WHEN handler_name = 'RYDER' THEN 'Ryder Corporate'
                -- Everything else
                ELSE 'Ryder Other'
            END as business_unit,
            COUNT(*) as cnt
        FROM HD_HANDLER 
        WHERE current_record='Y' AND ({where_final})
        GROUP BY 1
        ORDER BY cnt DESC
    """, "Refined business unit categorization")
    
    # Final validation - check sample records for each category
    run_query(f"""
        SELECT 
            CASE 
                WHEN handler_name ILIKE '%TRUCK RENTAL%' THEN 'Ryder Truck Rental'
                WHEN handler_name ILIKE '%TRANSPORTATION SERVICES%' OR handler_name ILIKE '%TRANSPORTATION SERVICE%' THEN 'Ryder Transportation Services'
                WHEN handler_name ILIKE '%TRANSPORTATION SVCS%' OR handler_name ILIKE '%TRANSPORTATION SVC%' THEN 'Ryder Transportation Services'
                WHEN handler_name ILIKE '%INTEGRATED LOGISTICS%' THEN 'Ryder Integrated Logistics'
                WHEN handler_name ILIKE '%STUDENT TRANSPORTATION%' THEN 'Ryder Student Transportation'
                WHEN handler_name ILIKE '%TRANSPORTATION%' THEN 'Ryder Transportation Other'
                WHEN handler_name ILIKE '%SYSTEMS%' THEN 'Ryder Systems'
                WHEN handler_name ILIKE '%TRUCK%' AND handler_name NOT ILIKE '%RENTAL%' THEN 'Ryder Truck Other'
                WHEN handler_name ILIKE '%LOGISTICS%' THEN 'Ryder Logistics Other'
                WHEN handler_name = 'RYDER' THEN 'Ryder Corporate'
                ELSE 'Ryder Other'
            END as business_unit,
            handler_name,
            location_city,
            location_state
        FROM HD_HANDLER 
        WHERE current_record='Y' AND ({where_final})
        ORDER BY business_unit, handler_name
        LIMIT 25
    """, "Sample records by business unit")
    
    print("\n" + "=" * 60)
    print("FINAL QUERY DEVELOPMENT")
    print("=" * 60)
    
    # Build the complete final query with all components
    final_complete_query = f"""
        SELECT handler_id
            ,activity_location
            ,source_type
            ,seq_number
            ,receive_date
            ,handler_name
            ,CASE 
                WHEN handler_name ILIKE '%TRUCK RENTAL%' THEN 'Ryder Truck Rental'
                WHEN handler_name ILIKE '%TRANSPORTATION SERVICES%' OR handler_name ILIKE '%TRANSPORTATION SERVICE%' THEN 'Ryder Transportation Services'
                WHEN handler_name ILIKE '%TRANSPORTATION SVCS%' OR handler_name ILIKE '%TRANSPORTATION SVC%' THEN 'Ryder Transportation Services'
                WHEN handler_name ILIKE '%INTEGRATED LOGISTICS%' THEN 'Ryder Integrated Logistics'
                WHEN handler_name ILIKE '%STUDENT TRANSPORTATION%' THEN 'Ryder Student Transportation'
                WHEN handler_name ILIKE '%TRANSPORTATION%' THEN 'Ryder Transportation Other'
                WHEN handler_name ILIKE '%SYSTEMS%' THEN 'Ryder Systems'
                WHEN handler_name ILIKE '%TRUCK%' AND handler_name NOT ILIKE '%RENTAL%' THEN 'Ryder Truck Other'
                WHEN handler_name ILIKE '%LOGISTICS%' THEN 'Ryder Logistics Other'
                WHEN handler_name = 'RYDER' THEN 'Ryder Corporate'
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
        WHERE ({where_final})
            AND current_record='Y'
    """
    
    run_query(final_complete_query, "Complete final Ryder query")
    
    # Test summary by business unit and generator status
    run_query(f"""
        SELECT business_unit, generator_status, COUNT(*) as facilities
        FROM (
            {final_complete_query}
        ) t
        GROUP BY business_unit, generator_status
        ORDER BY business_unit, facilities DESC
    """, "Summary by business unit and generator status")