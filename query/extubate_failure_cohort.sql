DROP MATERIALIZED VIEW IF EXISTS ventilation.extubate_failure_cohort;
CREATE MATERIALIZED VIEW ventilation.extubate_failure_cohort AS
WITH ext AS
(
    SELECT DISTINCT
        subject_id
        , hadm_id
		, stay_id
        , charttime AS extubate_time 
    FROM ventilation.in_extubation
    WHERE procedure = 'Extubation'
--     LIMIT 10
)
, co AS
(
    SELECT 
        ext.*
		, age.age
        , rank() OVER(
            PARTITION BY ext.hadm_id
            ORDER BY ext.extubate_time
           ) AS ext_num
		, ext.extubate_time - interval '4' hour AS starttime
		, ext.extubate_time AS endtime
    FROM ext
    LEFT JOIN mimic_derived.age_info age
    ON ext.hadm_id = age.hadm_id
)
SELECT 
	*
	, CASE WHEN age<18 OR ext_num>1 THEN 1
	ELSE 0 END AS excluded
FROM co
