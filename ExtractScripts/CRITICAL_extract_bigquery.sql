/**
OMOP extraction code for CRITICAL - BigQuery 
Robert Miller (Tufts) - 05/12/2022

SCRIPT ASSUMPTIONS:
1. You have already created a table 'CRITICAL_COHORT' consisting of patients that fit the CRITICAL cohort definition
	-the cohort table should consist of a single column 'person_id'
	-the cohort table resides within the schema defined by the "cohortDatabaseSchema" parameter specified in the R script

**/

--PERSON
--OUTPUT_FILE: PERSON.csv
select
   p.person_id,
   gender_concept_id,
   IFNULL(year_of_birth,extract(year from birth_datetime)) as year_of_birth,
   IFNULL(month_of_birth,extract(month from birth_datetime)) as month_of_birth,
   race_concept_id,
   ethnicity_concept_id,
   location_id,
   provider_id,
   care_site_id,
   null as person_source_value,
   gender_source_value,
   race_source_value,
   race_source_concept_id,
   ethnicity_source_value,
   ethnicity_source_concept_id
  from @cdmDatabaseSchema.person p
  join @cohortDatabaseSchema.critical_cohort n
    on p.person_id = n.person_id;

--OBSERVATION_PERIOD
--OUTPUT_FILE: OBSERVATION_PERIOD.csv
select
   observation_period_id,
   p.person_id,
   cast(observation_period_start_date as datetime) as observation_period_start_date,
   cast(observation_period_end_date as datetime) as observation_period_end_date,
   period_type_concept_id
 from @cdmDatabaseSchema.observation_period p
 join @cohortDatabaseSchema.critical_cohort n
   on p.person_id = n.person_id
   and (p.observation_period_start_date >= TO_DATE(TO_CHAR(2018,'0000')||'-'||TO_CHAR(01,'00')||'-'||TO_CHAR(01,'00'), 'YYYY-MM-DD') OR p.observation_period_end_date >= TO_DATE(TO_CHAR(2018,'0000')||'-'||TO_CHAR(01,'00')||'-'||TO_CHAR(01,'00'), 'YYYY-MM-DD'));

--VISIT_OCCURRENCE
--OUTPUT_FILE: VISIT_OCCURRENCE.csv
select
   visit_occurrence_id,
   n.person_id,
   visit_concept_id,
   cast(visit_start_date as datetime) as visit_start_date,
   cast(visit_start_datetime as datetime) as visit_start_datetime,
   cast(visit_end_date as datetime) as visit_end_date,
   cast(visit_end_datetime as datetime) as visit_end_datetime,
   visit_type_concept_id,
   provider_id,
   care_site_id,
   visit_source_value,
   visit_source_concept_id,
   admitting_source_concept_id,
   admitting_source_value,
   discharge_to_concept_id,
   discharge_to_source_value,
   preceding_visit_occurrence_id
from @cdmDatabaseSchema.visit_occurrence v
join @cohortDatabaseSchema.critical_cohort n
  on v.person_id = n.person_id;

--CONDITION_OCCURRENCE
--OUTPUT_FILE: CONDITION_OCCURRENCE.csv
select
   condition_occurrence_id,
   n.person_id,
   condition_concept_id,
   cast(condition_start_date as datetime) as condition_start_date,
   cast(condition_start_datetime as datetime) as condition_start_datetime,
   cast(condition_end_date as datetime) as condition_end_date,
   cast(condition_end_datetime as datetime) as condition_end_datetime,
   condition_type_concept_id,
   condition_status_concept_id,
   null as stop_reason,
   visit_occurrence_id,
   null as visit_detail_id,
   condition_source_value,
   condition_source_concept_id,
   null as condition_status_source_value
from @cdmDatabaseSchema.condition_occurrence co
join @cohortDatabaseSchema.critical_cohort n
  on co.person_id = n.person_id;

--DRUG_EXPOSURE
--OUTPUT_FILE: DRUG_EXPOSURE.csv
select
   drug_exposure_id,
   n.person_id,
   drug_concept_id,
   cast(drug_exposure_start_date as datetime) as drug_exposure_start_date,
   cast(drug_exposure_start_datetime as datetime) as drug_exposure_start_datetime,
   cast(drug_exposure_end_date as datetime) as drug_exposure_end_date,
   cast(drug_exposure_end_datetime as datetime) as drug_exposure_end_datetime,
   drug_type_concept_id,
   null as stop_reason,
   refills,
   quantity,
   days_supply,
   null as sig,
   route_concept_id,
   lot_number,
   provider_id,
   visit_occurrence_id,
   null as visit_detail_id,
   drug_source_value,
   drug_source_concept_id,
   route_source_value,
   dose_unit_source_value
from @cdmDatabaseSchema.drug_exposure de
join @cohortDatabaseSchema.critical_cohort n
  on de.person_id = n.person_id;

--DEVICE_EXPOSURE
--OUTPUT_FILE: DEVICE_EXPOSURE.csv
SELECT
   DEVICE_EXPOSURE_ID,
   n.PERSON_ID,
   DEVICE_CONCEPT_ID,
   CAST(DEVICE_EXPOSURE_START_DATE as datetime) as DEVICE_EXPOSURE_START_DATE,
   CAST(DEVICE_EXPOSURE_START_DATETIME as datetime) as DEVICE_EXPOSURE_START_DATETIME,
   CAST(DEVICE_EXPOSURE_END_DATE as datetime) as DEVICE_EXPOSURE_END_DATE,
   CAST(DEVICE_EXPOSURE_END_DATETIME as datetime) as DEVICE_EXPOSURE_END_DATETIME,
   DEVICE_TYPE_CONCEPT_ID,
   NULL as UNIQUE_DEVICE_ID,
   QUANTITY,
   PROVIDER_ID,
   VISIT_OCCURRENCE_ID,
   NULL as VISIT_DETAIL_ID,
   DEVICE_SOURCE_VALUE,
   DEVICE_SOURCE_CONCEPT_ID
FROM @cdmDatabaseSchema.device_exposure de
JOIN @cohortDatabaseSchema.critical_cohort n
  ON de.PERSON_ID = n.PERSON_ID;

--PROCEDURE_OCCURRENCE
--OUTPUT_FILE: PROCEDURE_OCCURRENCE.csv
select
   procedure_occurrence_id,
   n.person_id,
   procedure_concept_id,
   cast(procedure_date as datetime) as procedure_date,
   cast(procedure_datetime as datetime) as procedure_datetime,
   procedure_type_concept_id,
   modifier_concept_id,
   quantity,
   provider_id,
   visit_occurrence_id,
   null as visit_detail_id,
   procedure_source_value,
   procedure_source_concept_id,
   null as modifier_source_value
from @cdmDatabaseSchema.procedure_occurrence po
join @cohortDatabaseSchema.critical_cohort n
  on po.person_id = n.person_id;

--MEASUREMENT
--OUTPUT_FILE: MEASUREMENT.csv
select
   measurement_id,
   n.person_id,
   measurement_concept_id,
   cast(measurement_date as datetime) as measurement_date,
   cast(measurement_datetime as datetime) as measurement_datetime,
   null as measurement_time,
   measurement_type_concept_id,
   operator_concept_id,
   value_as_number,
   value_as_concept_id,
   unit_concept_id,
   range_low,
   range_high,
   provider_id,
   visit_occurrence_id,
   null as visit_detail_id,
   measurement_source_value,
   measurement_source_concept_id,
   null as unit_source_value,
   null as value_source_value
from @cdmDatabaseSchema.measurement m
join @cohortDatabaseSchema.critical_cohort n
  on m.person_id = n.person_id;

--OBSERVATION
--OUTPUT_FILE: OBSERVATION.csv
select
   observation_id,
   n.person_id,
   observation_concept_id,
   cast(observation_date as datetime) as observation_date,
   cast(observation_datetime as datetime) as observation_datetime,
   observation_type_concept_id,
   value_as_number,
   value_as_string,
   value_as_concept_id,
   qualifier_concept_id,
   unit_concept_id,
   provider_id,
   visit_occurrence_id,
   null as visit_detail_id,
   observation_source_value,
   observation_source_concept_id,
   null as unit_source_value,
   null as qualifier_source_value
from @cdmDatabaseSchema.observation o
join @cohortDatabaseSchema.critical_cohort n
  on o.person_id = n.person_id;

--DEATH
--OUTPUT_FILE: DEATH.csv
select
   n.person_id,
    cast(death_date as datetime) as death_date,
	cast(death_datetime as datetime) as death_datetime,
	death_type_concept_id,
	cause_concept_id,
	null as cause_source_value,
	cause_source_concept_id
from @cdmDatabaseSchema.death d
join @cohortDatabaseSchema.critical_cohort n
on d.person_id = n.person_id;

--LOCATION
--OUTPUT_FILE: LOCATION.csv
select
   l.location_id,
   null as address_1, -- to avoid identifying information
   null as address_2, -- to avoid identifying information
   city,
   state,
   zip,
   county,
   null as location_source_value
from @cdmDatabaseSchema.location l
join (
        select distinct p.location_id
        from @cdmDatabaseSchema.person p
        join @cohortDatabaseSchema.critical_cohort n
          on p.person_id = n.person_id
      ) a
  on l.location_id = a.location_id
;

--CARE_SITE
--OUTPUT_FILE: CARE_SITE.csv
select
   cs.care_site_id,
   care_site_name,
   place_of_service_concept_id,
   null as location_id,
   null as care_site_source_value,
   null as place_of_service_source_value
from @cdmDatabaseSchema.care_site cs
join (
        select distinct care_site_id
        from @cdmDatabaseSchema.visit_occurrence vo
        join @cohortDatabaseSchema.critical_cohort n
          on vo.person_id = n.person_id
      ) a
  on cs.care_site_id = a.care_site_id
;

--PROVIDER
--OUTPUT_FILE: PROVIDER.csv
select
   pr.provider_id,
   null as provider_name, -- to avoid accidentally identifying sites
   null as npi, -- to avoid accidentally identifying sites
   null as dea, -- to avoid accidentally identifying sites
   specialty_concept_id,
   care_site_id,
   null as year_of_birth,
   gender_concept_id,
   null as provider_source_value, -- to avoid accidentally identifying sites
   specialty_source_value,
   specialty_source_concept_id,
   gender_source_value,
   gender_source_concept_id
from @cdmDatabaseSchema.provider pr
join (
       select distinct provider_id
       from @cdmDatabaseSchema.visit_occurrence vo
       join @cohortDatabaseSchema.critical_cohort n
          on vo.person_id = n.person_id
       union distinct select distinct provider_id
       from @cdmDatabaseSchema.drug_exposure de
       join @cohortDatabaseSchema.critical_cohort n
          on de.person_id = n.person_id
       union distinct select distinct provider_id
       from @cdmDatabaseSchema.measurement m
       join @cohortDatabaseSchema.critical_cohort n
          on m.person_id = n.person_id
       union distinct select distinct provider_id
       from @cdmDatabaseSchema.procedure_occurrence po
       join @cohortDatabaseSchema.critical_cohort n
          on po.person_id = n.person_id
       union distinct select distinct provider_id
       from @cdmDatabaseSchema.observation o
       join @cohortDatabaseSchema.critical_cohort n
          on o.person_id = n.person_id
     ) a
 on pr.provider_id = a.provider_id
;

--DRUG_ERA
--OUTPUT_FILE: DRUG_ERA.csv
select
   drug_era_id,
   n.person_id,
   drug_concept_id,
   cast(drug_era_start_date as datetime) as drug_era_start_date,
   cast(drug_era_end_date as datetime) as drug_era_end_date,
   drug_exposure_count,
   gap_days
from @cdmDatabaseSchema.drug_era dre
join @cohortDatabaseSchema.critical_cohort n
  on dre.person_id = n.person_id;

--CONDITION_ERA
--OUTPUT_FILE: CONDITION_ERA.csv
select
   condition_era_id,
   n.person_id,
   condition_concept_id,
   cast(condition_era_start_date as datetime) as condition_era_start_date,
   cast(condition_era_end_date as datetime) as condition_era_end_date,
   condition_occurrence_count
from @cdmDatabaseSchema.condition_era ce 
join @cohortDatabaseSchema.critical_cohort n 
on ce.person_id = n.person_id;

--VISIT_DETAIL
--OUTPUT_FILE: VISIT_DETAIL.csv
select visit_detail_id
      ,v.person_id
      ,visit_detail_concept_id
      ,cast(visit_detail_start_date as datetime) as visit_detail_start_date
      ,cast(visit_detail_start_datetime as datetime) as visit_detail_start_datetime
      ,cast(visit_detail_end_date as datetime) as visit_detail_end_date
      ,cast(visit_detail_end_datetime as datetime) as visit_detail_end_datetime
      ,visit_detail_type_concept_id
      ,provider_id
      ,care_site_id
      ,visit_detail_source_value
      ,visit_detail_source_concept_id
      ,admitted_from_concept_id
      ,admitted_from_source_value
      ,discharged_to_source_value
      ,discharged_to_concept_id
      ,preceding_visit_detail_id
      ,parent_visit_detail_id
      ,visit_occurrence_id
  from @cdmDatabaseSchema.visit_detail v
  join @cohortDatabaseSchema.critical_cohort n
  on v.person_id = n.person_id;