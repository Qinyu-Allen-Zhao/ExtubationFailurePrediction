DROP MATERIALIZED VIEW IF EXISTS ventilation.extubate_failure_label CASCADE;
CREATE MATERIALIZED VIEW ventilation.extubate_failure_label AS
WITH co AS
(
    SELECT 
        *
    FROM ventilation.extubate_failure_cohort 
    WHERE excluded=0
)
, re AS
(
	SELECT
		co.hadm_id
		, MIN(ie.charttime) AS reintubate_time
	FROM co
	LEFT JOIN ventilation.in_extubation ie
    ON ie.procedure = 'Intubation'
	AND ie.hadm_id = co.hadm_id
    AND ie.charttime <= co.extubate_time + interval '28' day
    AND ie.charttime > co.extubate_time
	GROUP BY co.hadm_id
)
, niv AS
(
	SELECT 
		co.hadm_id
		, MIN(vd.starttime) AS niv_time
        , SUM( CAST(EXTRACT(epoch FROM vd.endtime-vd.starttime)/(60*60*24) AS numeric) ) AS niv_duration
	FROM co
	LEFT JOIN mimic_derived.ventilation_durations vd
	ON co.stay_id = vd.stay_id
    AND vd.starttime >= co.extubate_time
    AND vd.ventilation_status = 'NonInvasiveVent'
--     AND vd.ventilation_status in ('NonInvasiveVent', 'HighFlow')
    GROUP BY co.hadm_id
)

SELECT 
	co.hadm_id
	, co.stay_id
	, ad.deathtime
    , ROUND( (CAST(EXTRACT(epoch FROM ad.deathtime-co.extubate_time)/(60*60*24) AS numeric)), 4)
    AS time_to_death
    , ad.dischtime
    , ROUND( (CAST(EXTRACT(epoch FROM ad.dischtime-co.extubate_time)/(60*60*24) AS numeric)), 4)
    AS time_to_disch
	, re.reintubate_time
    , ROUND( (CAST(EXTRACT(epoch FROM re.reintubate_time - co.extubate_time)/(60*60*24) AS numeric)), 4)
    AS time_to_reintubation
	, niv.niv_time
    , niv.niv_duration
	, ROUND( (CAST(EXTRACT(epoch FROM niv.niv_time - co.extubate_time)/(60*60*24) AS numeric)), 4)
    AS time_to_niv
FROM co
LEFT JOIN mimic_core.admissions ad
    ON co.hadm_id = ad.hadm_id
LEFT JOIN re
	ON co.hadm_id = re.hadm_id
LEFT JOIN niv
	ON co.hadm_id = niv.hadm_id