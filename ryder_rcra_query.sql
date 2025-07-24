-- Final Ryder RCRA Query
-- Developed through iterative analysis for comprehensive coverage
-- Total facilities: 948

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
WHERE (
        handler_name ILIKE '%RYDER%' 
        OR contact_email_address ILIKE '%@ryder.com'
        OR contact_email_address ILIKE '%@ryderheil.com'
        OR contact_email_address ILIKE '%@ryderfs.com'
    )
    AND current_record='Y'