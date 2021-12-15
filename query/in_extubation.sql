DROP MATERIALIZED VIEW IF EXISTS ventilation.in_extubation CASCADE;
CREATE MATERIALIZED VIEW ventilation.in_extubation AS
-- 227194	Extubation
-- 225448	Percutaneous Tracheostomy
-- 225468	Unplanned Extubation (patient-initiated)
-- 225477	Unplanned Extubation (non-patient initiated)
-- 224385	Intubation
-- 226237	Open Tracheostomy
SELECT 
	subject_id
    , hadm_id
    , stay_id
    , starttime AS charttime
    , CASE WHEN itemid = 227194 THEN 'Extubation'
         WHEN itemid = 225448 THEN 'Percutaneous Tracheostomy'
         WHEN itemid = 225468 THEN 'Unplanned Extubation (patient-initiated)'
         WHEN itemid = 225477 THEN 'Unplanned Extubation (non-patient initiated)'
         WHEN itemid = 224385 THEN 'Intubation'
         WHEN itemid = 226237 THEN 'Open Tracheostomy'
         ELSE NULL END AS procedure
FROM mimic_icu.procedureevents pd
WHERE itemid in (227194, 225448, 225468, 225477, 224385, 226237);
