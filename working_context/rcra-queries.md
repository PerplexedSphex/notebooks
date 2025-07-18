```sql
SET regexp_pattern = '
(UN\\d{4}),\\s*                                       -- Group 1: UN Number (e.g., UN1993)
(.+?)                                               -- Group 2: Proper Shipping Name (non-greedy, gets everything until next comma or
(?:\\s*\\((.*?)\\))?                                -- Group 3: Optional Technical Names (e.g., (METHANOL, HEXANE))
,\\s*
(\\d{1,2}(?:\\.\\d)?)                                  -- Group 4: Primary Hazard Class (e.g., 3, 4.3)
(?:\\s*\\((.*?)\\))?                                   -- Group 5: Optional Subsidiary Hazard Class (e.g., (8))
,?\\s*                                               -- Optional comma and space
(?:PG\\s*)?                                          -- Optional "PG " literal
([IVX]{1,3})                                        -- Group 6: Packing Group (e.g., I, II, III)
'
;

with account_facilities as (select handler_name
from account_matched_facilities	
where account_id='0011U00001gwOtcQAE'
order by handler_name
)
,
business_units as (
SELECT
	handler_id,
    handler_name,
    CASE
        -- Specific Business Units discussed or clearly identifiable
        WHEN handler_name LIKE 'RED-D-ARC%' OR handler_name LIKE 'RED D ARC%' THEN 'RED-D-ARC'
        WHEN handler_name LIKE '%WELD%' THEN 'NATIONAL WLEDERS'
        WHEN handler_name LIKE 'NITROUS OXIDE%' THEN 'NITROUS OXIDE CORP'
        WHEN handler_name LIKE 'AIRGAS%SAFETY%' THEN 'AIRGAS SAFETY'
        WHEN handler_name LIKE 'AIRGAS SPECIALTY%' THEN 'AIRGAS SPECIALTY'
        WHEN handler_name LIKE '%CARBONIC%' THEN 'AIRGAS CARBONIC'
        
        -- Primary Operating Company and Corporate Entities
        WHEN handler_name LIKE 'AIRGAS USA%' OR handler_name LIKE 'AIRGAS US %' THEN 'AIRGAS USA, LLC'
        WHEN handler_name LIKE 'AIRGAS, INC%' OR handler_name LIKE 'AIRGAS INC%' THEN 'AIRGAS, INC.'
        WHEN handler_name LIKE 'AIR LIQUIDE%' OR handler_name LIKE 'AIRLIQUIDE%' THEN 'AIR LIQUIDE'
        
        
        -- Consolidated Regional Group
        WHEN handler_name LIKE '%NORTHEAST%'
             OR handler_name LIKE '%NORTH CENTRAL%'
             OR handler_name LIKE '%INTERMOUNTAIN%'
             OR handler_name LIKE '%GREAT LAKES%'
             OR handler_name LIKE '%EAST%'
             OR handler_name LIKE '%AIRGAS WEST%'
             OR handler_name LIKE '%MID AMERICA%'
             OR handler_name LIKE '%SOUTH%'            
        THEN 'AIRGAS REGIONAL'
        
        -- Generic Plant Category (as requested)
        WHEN handler_name LIKE '%PLANT%' THEN 'PLANT'
        
        -- Catch-all for other 'Airgas' entities
        WHEN handler_name LIKE 'AIRGAS' THEN 'AIRGAS - GENERAL'
        
        ELSE 'OTHER'
    END AS "business_unit",
--     split_part(contact_email_address,'@',2) as contact_domain,
    TRIM(CONCAT_WS(' ', location_street_no, location_street1)) AS street_address,
    location_city,
        location_state,
        location_zip,
        case when fed_waste_generator='1' then 'LQG' 
        when fed_waste_generator='2' then 'SQG'
        when fed_waste_generator='3' then 'VSQG'
        when fed_waste_generator='N' then 'NON REPORTER'
        else 'OTHER' end as generator_status,
--         contact_email_address
FROM
    HD_HANDLER
WHERE current_record and
    -- This WHERE clause selects all records that will be categorized by the CASE statement,
    -- EXCLUDING the generic 'PLANT' category as requested.
    (
        handler_name LIKE 'RED-D-ARC%'
        OR handler_name LIKE 'RED D ARC%'
        OR handler_name LIKE 'NITROUS OXIDE CORP%'
        OR handler_name LIKE 'AIR LIQUIDE%'
        OR handler_name LIKE 'AIRLIQUIDE%'
        OR handler_name LIKE 'AIRGAS%' -- This final condition catches the plain 'AIRGAS' entries.
        OR contact_email_address like '%@AIRGAS.COM'
        OR contact_email_address like '%@AIRLIQUIDE.COM'
        
    )

)
,
regional_deep as (
	select * from business_units where business_unit in ('AIRGAS REGIONAL', 'AIRGAS - GENERAL') order by handler_name
)
,
domains_deep as (
	select contact_domain, count(distinct handler_id) as fac_count from business_units group by contact_domain order by fac_count desc
)
,
other_deep as (
	select * from business_units 
	where (
	business_unit='OTHER' or business_unit='AIRGAS - GENERAL')
	and handler_name not like 'AIRGAS'
	)
,
business_unit_count as (
select business_unit, count(DISTINCT handler_id) as fac_count from business_units group by business_unit order by fac_count desc
)
,
manifests as (
	select manifest_tracking_number
		,shipped_date
		,business_unit
		,generator_id
		,generator_name
		,generator_location_state
		,generator_contact_company_name
		,des_facility_id
		,des_facility_name
		,des_fac_location_state
		,total_quantity_haz_tons
		,total_quantity_non_haz_tons
		,total_quantity_tons
	from EM_MANIFEST a join business_units b on generator_id=handler_id
	where SUBSTRING(shipped_date,1,4) = '2024'
)
,
manifest_stats as (
	select count(DISTINCT generator_id) as generators_with_manifests
		,sum(manifest_count) as manifest_count
		,avg(manifest_count) as avg_manifests_per_generator
		,median(manifest_count) as median_manifests_per_generator
		,round(sum(shipped_tons)) as total_waste_shipped
		,round(avg(shipped_tons)) as avg_waste_shipped_per_generator
		,round(median(shipped_tons)) as median_waste_shipped_per_generator
	from (
		select generator_id
			,count(DISTINCT manifest_tracking_number) as manifest_count
			,sum(cast(total_quantity_tons as double)) as shipped_tons
		from manifests
		group by generator_id
	)
)
,
transporters as (
	select 'Transport Facility' as vendor_type
-- 		,generator_location_state
		,b.handler_name
		,split_part(b.contact_email_address,'@',2) as contact_email_domain
		,count(DISTINCT a.manifest_tracking_number) as manifest_count
	from EM_TRANSPORTER a 
		join HD_HANDLER b on a.transporter_id=b.handler_id
		join manifests c on a.manifest_tracking_number=c.manifest_tracking_number
	where current_record='Y'
	group by 
-- 	generator_location_state, 
	handler_name, contact_email_domain
	order by 
-- 	generator_location_state, 
	manifest_count desc
)
,
disposal_facilities as (
	select 'Disposal Facility' as vendor_type
-- 		,generator_location_state
		,b.handler_name
		,split_part(b.contact_email_address,'@',2) as contact_email_domain
		,count(DISTINCT manifest_tracking_number) as manifest_count
	from manifests a 
		join HD_HANDLER b on a.des_facility_id=b.handler_id
	where current_record='Y'
	group by 
-- 	generator_location_state, 
	handler_name, contact_email_domain
	order by 
-- 	generator_location_state, 
	manifest_count desc
)
,
vendors as (
	select * from transporters
	union all
	select * from disposal_facilities
)
,
manifest_lines as (
	select * from EM_WASTE_LINE a join manifests b on a.manifest_tracking_number=b.manifest_tracking_number
)



select dot_printed_information,
	non_haz_waste_description,
	regexp_extract(dot_printed_information, regexp_pattern, 1) AS "UN Number",
    regexp_extract(dot_printed_information, regexp_pattern, 2) AS "Proper Shipping Name",
    regexp_extract(dot_printed_information, regexp_pattern, 3) AS "Technical Names",
    regexp_extract(dot_printed_information, regexp_pattern, 4) AS "Primary Hazard Class",
    regexp_extract(dot_printed_information, regexp_pattern, 5) AS "Subsidiary Hazard Class",
    regexp_extract(dot_printed_information, regexp_pattern, 6) AS "Packing Group",
    cont(DISTINCT manifest_tracking_number)
from manifest_lines;


-- select * from manifest_stats
-- select * from transporters
-- select * from vendors

-- select * from business_units 
-- select * from business_unit_count
-- select facility_state, count(distinct handler_id) as fac_count from business_units group by facility_state order by fac_count desc
-- select generator_status, count(distinct handler_id) as fac_count from business_units group by generator_status order by fac_count desc
-- select business_unit, facility_state, count(DISTINCT handler_id) as fac_count from business_units group by business_unit, facility_state order by fac_count desc
-- select business_unit, generator_status, count(DISTINCT handler_id) as fac_count from business_units group by business_unit, generator_status order by fac_count desc

-- select a.*, b.fac_count as bu_total_tacs from (select business_unit, facility_state, count(DISTINCT handler_id) as fac_count from business_units group by business_unit, facility_state) a join business_unit_count b on a.business_unit=b.business_unit order by bu_total_tacs desc, a.fac_count desc

-- select a.*, b.fac_count as bu_total_tacs from (select business_unit, generator_status, count(DISTINCT handler_id) as fac_count from business_units group by business_unit, generator_status) a join business_unit_count b on a.business_unit=b.business_unit order by bu_total_tacs desc, a.fac_count desc

-- select business_unit, generator_status, count(DISTINCT handler_id) as fac_count from business_units group by business_unit, generator_status order by fac_count desc
-- select * from regional_deep
-- select * from domains_deep
-- select * from other_deep

```

