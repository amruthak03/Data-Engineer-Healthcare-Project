--- Create table 'encounters' in silver layer. 
CREATE TABLE IF NOT EXISTS `project-15f498fb-28c2-4528-bc7.silver_dataset.encounters` (
    Encounter_Key STRING,
    SRC_EncounterID STRING,
    PatientID STRING,
    ProviderID STRING,
    DepartmentID STRING,
    EncounterDate INT64,
    EncounterType STRING,
    ProcedureCode INT64,
    SRC_ModifiedDate INT64,
    datasource STRING,
    is_quarantined BOOL,
    inserted_date TIMESTAMP,
    modified_date TIMESTAMP,
    is_current BOOL
);

---- Create a quality checks temp table
CREATE OR REPLACE TABLE `project-15f498fb-28c2-4528-bc7.silver_dataset.quality_checks_encounters` AS
SELECT DISTINCT 
    CONCAT(SRC_EncounterID, '-', datasource) AS Encounter_Key,
    SRC_EncounterID,
    PatientID,
    ProviderID,
    DepartmentID,
    EncounterDate,
    EncounterType,
    ProcedureCode,
    ModifiedDate AS SRC_ModifiedDate,
    datasource,
    CASE 
        WHEN SRC_EncounterID IS NULL OR PatientID IS NULL OR EncounterDate IS NULL OR LOWER(EncounterType) = 'null' THEN TRUE
        ELSE FALSE
    END AS is_quarantined
FROM (
    SELECT DISTINCT 
        EncounterID AS SRC_EncounterID,
        PatientID,
        ProviderID,
        DepartmentID,
        EncounterDate,
        EncounterType,
        ProcedureCode,
        ModifiedDate,
        'hosa' AS datasource
    FROM `project-15f498fb-28c2-4528-bc7.bronze_dataset.encounters_ha`
    
    UNION ALL

    SELECT DISTINCT 
        EncounterID AS SRC_EncounterID,
        PatientID,
        ProviderID,
        DepartmentID,
        EncounterDate,
        EncounterType,
        ProcedureCode,
        ModifiedDate,
        'hosb' AS datasource
    FROM `project-15f498fb-28c2-4528-bc7.bronze_dataset.encounters_hb`
);

--- Implement SCD2 technique with MERGE
MERGE INTO `project-15f498fb-28c2-4528-bc7.silver_dataset.encounters` AS target
USING `project-15f498fb-28c2-4528-bc7.silver_dataset.quality_checks_encounters` AS source
ON target.Encounter_Key = source.Encounter_Key
AND target.is_current = TRUE 

--- step 1: when matched the existing records needs to be marked as historical if any column values are changed
WHEN MATCHED AND (
    target.SRC_EncounterID <> source.SRC_EncounterID OR
    target.PatientID <> source.PatientID OR
    target.ProviderID <> source.ProviderID OR
    target.DepartmentID <> source.DepartmentID OR
    target.EncounterDate <> source.EncounterDate OR
    target.EncounterType <> source.EncounterType OR
    target.ProcedureCode <> source.ProcedureCode OR
    target.SRC_ModifiedDate <> source.SRC_ModifiedDate OR
    target.datasource <> source.datasource OR
    target.is_quarantined <> source.is_quarantined
)
THEN UPDATE SET 
    target.is_current = FALSE,
    target.modified_date = CURRENT_TIMESTAMP()

---- step 2: Insert new and upadated records as the latest active records when not matched
WHEN NOT MATCHED 
THEN INSERT (
    Encounter_Key,
    SRC_EncounterID,
    PatientID,
    ProviderID,
    DepartmentID,
    EncounterDate,
    EncounterType,
    ProcedureCode,
    SRC_ModifiedDate,
    datasource,
    is_quarantined,
    inserted_date,
    modified_date,
    is_current
)
VALUES (
    source.Encounter_Key,
    source.SRC_EncounterID,
    source.PatientID,
    source.ProviderID,
    source.DepartmentID,
    source.EncounterDate,
    source.EncounterType,
    source.ProcedureCode,
    source.SRC_ModifiedDate,
    source.datasource,
    source.is_quarantined,
    CURRENT_TIMESTAMP(),  
    CURRENT_TIMESTAMP(),  
    TRUE 
);

----- Drop quality_check table as we don't want it, we needed that only for that time
DROP TABLE IF EXISTS `project-15f498fb-28c2-4528-bc7.silver_dataset.quality_checks_encounters`;
