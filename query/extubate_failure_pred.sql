DROP MATERIALIZED VIEW IF EXISTS ventilation.extubate_failure_pred;
CREATE MATERIALIZED VIEW ventilation.extubate_failure_pred AS
WITH co AS
(
    SELECT 
        subject_id
        , hadm_id
        , stay_id
        , starttime
        , endtime
        , extubate_time
    FROM ventilation.extubate_failure_cohort 
    WHERE excluded=0
)
, sbt AS
(
    SELECT
        co.stay_id
        , COALESCE(SUM(sbt.success), 0) AS sbt_success_times
        , COALESCE(SUM(1-sbt.success), 0) AS sbt_failure_times
        , COALESCE(MAX(CASE WHEN sbt.charttime >= co.extubate_time - interval '24' hour 
              THEN sbt.success ELSE 0 END),0) AS sbt_24h
        , COALESCE(MAX(CASE WHEN sbt.charttime >= co.extubate_time - interval '12' hour 
              THEN sbt.success ELSE 0 END),0) AS sbt_12h
        , COALESCE(MAX(CASE WHEN sbt.charttime >= co.extubate_time - interval '4' hour 
              THEN sbt.success ELSE 0 END),0) AS sbt_4h
    FROM co
    LEFT JOIN ventilation.sbt sbt
    ON co.stay_id = sbt.stay_id
    AND co.extubate_time > sbt.charttime
    GROUP BY co.stay_id
)
, vi AS
(
    SELECT
        co.stay_id
        , AVG(heart_rate) AS heart_rate
        , AVG(resp_rate) AS resp_rate
        , AVG(COALESCE(mbp, mbp_ni)) AS mbp
        , AVG(COALESCE(sbp, sbp_ni)) AS sbp
        , AVG(COALESCE(dbp, dbp_ni)) AS dbp
        , AVG(temperature) AS temperature
        , AVG(spo2) AS spo2
    FROM co
    LEFT JOIN mimic_derived.vital_signs vi
    ON co.subject_id = vi.subject_id
    AND vi.charttime >= co.starttime 
    AND vi.charttime < co.endtime
    GROUP BY co.stay_id
)
, ch AS
(
    SELECT
        co.stay_id
        , AVG(ch.albumin) AS albumin
        , AVG(ch.globulin) AS globulin
        , AVG(ch.total_protein) AS total_protein
        , AVG(ch.aniongap) AS aniongap
        , AVG(ch.bicarbonate) As bicarbonate
        , AVG(ch.bun) AS bun
        , AVG(ch.calcium) AS calcium
        , AVG(ch.chloride) AS chloride
        , AVG(ch.creatinine) AS creatinine
        , AVG(ch.glucose) AS glucose
        , AVG(ch.potassium) AS potassium
    FROM co
    LEFT JOIN mimic_derived.chemistry ch
    ON co.subject_id = ch.subject_id
    AND ch.charttime >= co.starttime 
    AND ch.charttime < co.endtime
    GROUP BY co.stay_id
)
, enz AS
(
    SELECT
        co.stay_id
        , AVG(enz.alt) AS alt
        , AVG(enz.ast) AS ast
        , AVG(enz.alp) AS alp
        , AVG(enz.bilirubin_total) AS tbil
        , AVG(enz.bilirubin_direct) AS dbil
        , AVG(enz.bilirubin_indirect) AS ibil
    FROM co
    LEFT JOIN mimic_derived.enzyme enz
    ON co.subject_id = enz.subject_id
    AND enz.charttime >= co.starttime 
    AND enz.charttime < co.endtime
    GROUP BY co.stay_id
)
, vio AS
(
    SELECT
        co.stay_id
        , AVG(vio.cvp) AS cvp
    FROM co
    LEFT JOIN mimic_derived.vital_other vio
    ON co.subject_id = vio.subject_id
    AND vio.charttime >= co.starttime 
    AND vio.charttime < co.endtime
    AND vio.cvp < 30
    GROUP BY co.stay_id
)
, bg AS
(
    SELECT
        co.stay_id
        , AVG(ph) AS ph
        , AVG(po2) AS pao2
        , AVG(pco2) AS paco2
        , AVG(COALESCE(fio2_chartevents, fio2)) AS fio2
        , AVG(pao2fio2ratio) AS pao2fio2ratio
        , AVG(so2) AS so2
        , AVG(baseexcess) AS baseexcess
        , AVG(bicarbonate) AS bicarbonate
        , AVG(aado2_calc) AS aado2
        , AVG(totalco2) AS totalco2
        , AVG(hematocrit) AS hematocrit
        , AVG(hemoglobin) AS hemoglobin
        , AVG(chloride) AS chloride
        , AVG(calcium) AS calcium
        , AVG(potassium) AS potassium
        , AVG(sodium) AS sodium
        , AVG(lactate) AS lactate
        , AVG(glucose) AS glucose
    FROM co
    LEFT JOIN mimic_derived.bg bg
    ON co.subject_id = bg.subject_id
    AND bg.charttime >= co.starttime 
    AND bg.charttime < co.endtime
    AND bg.specimen_pred IN ('ART.', 'ART')
    GROUP BY co.stay_id
)
, gcs AS
(
    SELECT
        co.stay_id
        , MIN(gcs) AS gcs
    FROM co
    LEFT JOIN mimic_derived.gcs gcs
    ON gcs.subject_id = co.subject_id
    AND gcs.charttime >= co.starttime 
    AND gcs.charttime < co.endtime
    GROUP BY co.stay_id
)
, coag AS
(
    SELECT
        co.stay_id
        , AVG(coag.fibrinogen) AS fibrinogen
        , AVG(coag.inr) AS inr
        , AVG(coag.pt) AS pt
        , AVG(coag.ptt) AS ptt
    FROM co
    LEFT JOIN mimic_derived.coagulation coag
    ON co.subject_id = coag.subject_id
    AND coag.charttime >= co.starttime 
    AND coag.charttime < co.endtime
    GROUP BY co.stay_id
)
, lo AS
(
    SELECT
        co.stay_id
        , AVG(lo.nt_pro_bnp) AS nt_pro_bnp
    FROM co
    LEFT JOIN mimic_derived.lab_other lo
    ON co.subject_id = lo.subject_id
    AND lo.charttime >= co.starttime 
    AND lo.charttime < co.endtime
    GROUP BY co.stay_id
)
, cbc AS
(
    SELECT
        co.stay_id
        , AVG(cbc.hematocrit) AS hematocrit
        , AVG(cbc.hemoglobin) AS hemoglobin
        , AVG(cbc.mch) AS mch
        , AVG(cbc.mchc) AS mchc
        , AVG(cbc.mcv) AS mcv
        , AVG(cbc.platelet) AS platelet
        , AVG(cbc.rbc) AS rbc
        , AVG(cbc.rdw) AS rdw
        , AVG(cbc.wbc) As wbc
    FROM co
    LEFT JOIN mimic_derived.complete_blood_count cbc
    ON co.subject_id = cbc.subject_id
    AND cbc.charttime >= co.starttime 
    AND cbc.charttime < co.endtime
    GROUP BY co.stay_id
)
, uo AS
(
    SELECT
        co.stay_id
        , SUM(uo.urineoutput) AS urine_output
    FROM co
    LEFT JOIN mimic_derived.urine_output uo
    ON co.stay_id = uo.stay_id
    AND uo.charttime >= co.endtime - interval '1' day 
    AND uo.charttime < co.endtime
    GROUP BY co.stay_id
)
, vs AS
(
    SELECT
        co.stay_id
        , AVG(vs.plateau_pressure) AS plateau_pressure
        , AVG(CASE WHEN vs.tidal_volume_observed between 100 and 1000
              THEN vs.tidal_volume_observed
             ELSE NULL END) AS tidal_volume
        , AVG(vs.peep) AS peep
    FROM co
    LEFT JOIN mimic_derived.ventilator_setting vs
    ON vs.subject_id = co.subject_id
    AND vs.charttime >= co.starttime 
    AND vs.charttime < co.endtime
    GROUP BY co.stay_id
)
, vso AS
(
    SELECT
        co.stay_id
        , AVG(psv_level) AS psv_level
        , AVG(mean_airway_pressure) AS mean_airway_pressure
        , AVG(mip) AS mip
    FROM co
    LEFT JOIN ventilation.ventilator_other vs
    ON vs.subject_id = co.subject_id
    AND vs.charttime >= co.starttime 
    AND vs.charttime < co.endtime
    GROUP BY co.stay_id
)
, cry AS
(
    SELECT
        co.stay_id
        , SUM(bo.amount) AS cry_amount
    FROM co
    LEFT JOIN mimic_derived.crystalloid_bolus bo
    ON co.stay_id = bo.stay_id
    AND bo.charttime >= co.endtime - interval '1' day 
    AND bo.charttime < co.endtime
    GROUP BY co.stay_id
)
, col AS
(
    SELECT
        co.stay_id
        , SUM(bo.amount) AS col_amount
    FROM co
    LEFT JOIN mimic_derived.colloid_bolus bo
    ON co.stay_id = bo.stay_id
    AND bo.charttime >= co.endtime - interval '1' day 
    AND bo.charttime < co.endtime
    GROUP BY co.stay_id
)
, he AS
(
    SELECT 
        co.stay_id
        , h.height
        , ROW_NUMBER() OVER (
            PARTITION BY co.stay_id 
            ORDER BY ABS(extract(EPOCH from h.charttime-co.endtime)) ASC
        ) as rn
    FROM co
    LEFT JOIN mimic_derived.height h
    ON co.subject_id = h.subject_id
)
, bp AS
(
    SELECT
        co.stay_id
        , SUM(CASE WHEN blood_products = 'FFP' THEN amount ELSE 0 END) AS trans_ffp
        , SUM(CASE WHEN blood_products = 'platelet' THEN amount ELSE 0 END) AS trans_platelet
        , SUM(CASE WHEN blood_products = 'RBC' THEN amount ELSE 0 END) AS trans_rbc
    FROM co
    LEFT JOIN medication.blood_products bp
    ON co.stay_id = bp.stay_id
    AND bp.inputtime >= co.endtime - interval '1' day 
    AND bp.inputtime < co.endtime
    GROUP BY co.stay_id
)
, hp AS
(
    SELECT
        co.stay_id
        , MAX(CASE WHEN hp.stay_id is not null THEN 1 ELSE 0 END) AS heparin_use
    FROM co
    LEFT JOIN medication.heparin hp
    ON co.stay_id = hp.stay_id
    AND (
            (
            hp.starttime >= co.starttime 
            AND hp.starttime < co.endtime
            )
         OR
             (
            hp.endtime >= co.starttime 
            AND hp.endtime < co.endtime
             )
         )
    GROUP BY co.stay_id
)
, ab AS
(
    SELECT
        co.stay_id
        , COUNT(DISTINCT ab.antibiotic) AS antibiotic_types
        , MAX(CASE WHEN lower(antibiotic) like '%vancomycin%' THEN 1 
              ELSE 0 END) AS vancomycin
        , MAX(CASE WHEN lower(antibiotic) like '%linezolid%' THEN 1 
              ELSE 0 END) AS linezolid
    FROM co
    LEFT JOIN medication.antibiotic ab
    ON co.subject_id = ab.subject_id
    AND (
            (
            ab.starttime >= co.starttime 
            AND ab.starttime < co.endtime
            )
         OR
             (
            ab.stoptime >= co.starttime 
            AND ab.stoptime < co.endtime
             )
         )
    GROUP BY co.stay_id
)
, crrt AS
(
    SELECT 
        co.stay_id
        , MAX(CASE WHEN crrt.stay_id IS NOT NULL THEN 1 ELSE 0 END) AS crrt_use
    FROM co
    LEFT JOIN mimic_derived.crrt
    ON crrt.system_active = 1
    AND crrt.stay_id = co.stay_id
    AND crrt.charttime >= co.endtime - interval '24' hour
    AND crrt.charttime < co.endtime
    GROUP BY co.stay_id
)
, v AS
(
    SELECT stay_id, starttime, endtime FROM medication.dobutamine
    UNION
    SELECT stay_id, starttime, endtime FROM medication.dopamine
    UNION
    SELECT stay_id, starttime, endtime FROM medication.epinephrine
    UNION
    SELECT stay_id, starttime, endtime FROM medication.norepinephrine
    UNION
    SELECT stay_id, starttime, endtime FROM medication.phenylephrine
    UNION
    SELECT stay_id, starttime, endtime FROM medication.vasopressin
)
, vaso AS
(
    SELECT
        co.stay_id
        , MAX(CASE WHEN v.stay_id is not null THEN 1 ELSE 0 END) AS vaso_use
    FROM co
    LEFT JOIN v
    ON co.stay_id = v.stay_id
    AND (
            (
            v.starttime >= co.starttime 
            AND v.starttime < co.endtime
            )
         OR
             (
            v.endtime >= co.starttime 
            AND v.endtime < co.endtime
             )
         )
    GROUP BY co.stay_id
)
, vd AS
(
    SELECT 
        co.stay_id
        , SUM( CAST(EXTRACT(EPOCH FROM LEAST(vd.endtime, co.extubate_time) - vd.starttime)
                    / 3600.0 AS numeric)) AS vent_duration
    FROM co
    LEFT JOIN mimic_derived.ventilation_durations vd
    ON co.stay_id = vd.stay_id
    AND co.extubate_time > vd.starttime
    AND vd.ventilation_status = 'InvasiveVent'
    GROUP BY co.stay_id
)
SELECT 
	co.*
    , com.hypertension
    , com.DM
    , com.COPD
    , com.CHF
    , com.MI
    , com.CKD
    , com.leu
    , com.st
    , com.ca
    , com.ld
    , cl.charlson_comorbidity_index AS charlson
    
    , age.age
    , p.gender
    , p.anchor_year_group
    , ad.admission_type
    , ad.ethnicity
    , w.weight
    , he.height
    , w.weight / POWER(he.height/100, 2) AS bmi
    , saps.sapsii
    , COALESCE(sofa.respiration_24hours, 0) AS respiration_24hours
    , COALESCE(sofa.coagulation_24hours, 0) AS coagulation_24hours
    , COALESCE(sofa.liver_24hours, 0) AS liver_24hours
    , COALESCE(sofa.cardiovascular_24hours, 0) AS cardiovascular_24hours
    , COALESCE(sofa.cns_24hours, 0) AS cns_24hours
    , COALESCE(sofa.renal_24hours, 0) AS renal_24hours
    , COALESCE(sofa.sofa_24hours, 0) AS sofa_24hours
    , gcs.gcs
    , sbt.sbt_success_times
    , sbt.sbt_failure_times
    , sbt.sbt_24h
    , sbt.sbt_12h
    , sbt.sbt_4h
    
    , vi.heart_rate
    , vi.resp_rate
    , vi.mbp
    , vi.sbp
    , vi.dbp
    , vi.spo2
    , vi.temperature
    , vio.cvp
    
    , vs.plateau_pressure
    , vs.tidal_volume
    , vs.peep
    , vi.resp_rate / vs.tidal_volume * 1000 AS rsb
    , vso.psv_level
    , vso.mean_airway_pressure
    , vso.mip
    
    , ch.albumin
    , ch.total_protein
    , ch.aniongap
    , ch.bun
    , ch.creatinine
    , ch.globulin
    
    , coag.fibrinogen
    , coag.inr
    , coag.pt
    , coag.ptt
    
    , cbc.mch
    , cbc.mchc
    , cbc.mcv
    , cbc.platelet
    , cbc.rbc
    , cbc.rdw
    , cbc.wbc
    
    , enz.alt
    , enz.ast
    , enz.alp
    , enz.tbil
    , enz.dbil
    , enz.ibil
    
    , lo.nt_pro_bnp
    
    , bg.ph
    , bg.pao2
    , bg.paco2
    , bg.fio2
    , bg.pao2fio2ratio
    , bg.so2
    , bg.baseexcess
    , COALESCE(ch.bicarbonate, bg.bicarbonate) AS bicarbonate
    , bg.aado2
    , bg.totalco2
    , COALESCE(cbc.hematocrit, bg.hematocrit) AS hematocrit
    , COALESCE(cbc.hemoglobin, bg.hemoglobin) AS hemoglobin
    , COALESCE(ch.chloride, bg.chloride) AS chloride
    , COALESCE(ch.calcium, bg.calcium) AS calcium
    , COALESCE(ch.potassium, bg.potassium) AS potassium
    , bg.sodium
    , bg.lactate
    , COALESCE(ch.glucose, bg.glucose) AS clucose
    , bg.so2 / bg.fio2 AS spo2fio2ratio
    , bg.so2 / bg.fio2 / vi.resp_rate AS rox_index
    
    , LEAST(24, ROUND( CAST(EXTRACT(epoch FROM co.extubate_time-icu.intime)/3600.0 AS numeric), 4))
            AS io_time
    , COALESCE(uo.urine_output, 0) AS urine_output
    , COALESCE(col.col_amount, 0) AS col_amount
    , COALESCE(cry.cry_amount, 0) AS cry_amount
    , COALESCE(bp.trans_rbc, 0) AS trans_rbc
    , COALESCE(bp.trans_ffp, 0) AS trans_ffp
    , COALESCE(bp.trans_platelet, 0) AS trans_platelet
    , hp.heparin_use
    , ab.antibiotic_types
    , ab.linezolid
    , ab.vancomycin
    
    , crrt.crrt_use
    , vaso.vaso_use
    
    , vd.vent_duration
    
    , label.deathtime
    , label.time_to_death
    , label.dischtime
    , label.time_to_disch
    , label.reintubate_time
    , label.time_to_reintubation
    , label.niv_time
    , label.niv_duration
    , label.time_to_niv

FROM co
LEFT JOIN mimic_derived.comorbidity com
    ON co.hadm_id = com.hadm_id
LEFT JOIN mimic_derived.charlson cl
    ON co.hadm_id = cl.hadm_id
LEFT JOIN mimic_derived.age_info age
    ON co.hadm_id = age.hadm_id
LEFT JOIN mimic_core.admissions ad
    ON co.hadm_id = ad.hadm_id
LEFT JOIN mimic_icu.icustays icu
    ON co.stay_id = icu.stay_id
LEFT JOIN mimic_core.patients p
    ON co.subject_id = p.subject_id
LEFT JOIN mimic_derived.sofa sofa
    ON co.stay_id = sofa.stay_id
    AND co.extubate_time > sofa.starttime
    AND co.extubate_time <= sofa.endtime
LEFT JOIN vi 
    ON co.stay_id = vi.stay_id
LEFT JOIN sbt
    ON co.stay_id = sbt.stay_id
LEFT JOIN bg
    ON co.stay_id = bg.stay_id
LEFT JOIN gcs
    ON co.stay_id = gcs.stay_id
LEFT JOIN vs
    ON co.stay_id = vs.stay_id
LEFT JOIN mimic_derived.weight_durations w
    ON co.stay_id = w.stay_id
    AND co.endtime > w.starttime
    AND co.endtime <= w.endtime
LEFT JOIN he
    ON co.stay_id = he.stay_id
    AND he.rn = 1
LEFT JOIN vd
    ON co.stay_id = vd.stay_id
LEFT JOIN coag
    ON co.stay_id = coag.stay_id
LEFT JOIN ch
    ON co.stay_id = ch.stay_id
LEFT JOIN cbc
    ON co.stay_id = cbc.stay_id
LEFT JOIN enz
    ON co.stay_id = enz.stay_id
LEFT JOIN lo
    ON co.stay_id = lo.stay_id
LEFT JOIN vio
    ON co.stay_id = vio.stay_id
LEFT JOIN uo
    ON co.stay_id = uo.stay_id
LEFT JOIN cry
    ON co.stay_id = cry.stay_id
LEFT JOIN col
    ON co.stay_id = col.stay_id
LEFT JOIN bp
    ON co.stay_id = bp.stay_id
LEFT JOIN hp
    ON co.stay_id = hp.stay_id
LEFT JOIN ab
    ON co.stay_id = ab.stay_id
LEFT JOIN mimic_derived.sapsii saps
    ON co.stay_id = saps.stay_id
LEFT JOIN vso
    ON co.stay_id = vso.stay_id
LEFT JOIN ventilation.extubate_failure_label label
    ON co.stay_id = label.stay_id
LEFT JOIN crrt
    ON co.stay_id = crrt.stay_id
LEFT JOIN vaso
    ON co.stay_id = vaso.stay_id
;
