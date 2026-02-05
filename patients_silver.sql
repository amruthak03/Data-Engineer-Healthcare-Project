--- Now, for incremental table, say patients

--- Create Patients table in Silver datatset
CREATE TABLE IF NOT EXISTS `project-15f498fb-28c2-4528-bc7.silver_dataset.patients` (
  Patient_Key STRING,
  SRC_PatientID STRING,
  FirstName STRING,
  LastName STRING,
  MiddleName STRING,
  SSN STRING,
  PhoneNumber STRING,
  Gender STRING,
  DOB INT64,
  Address STRING,
  SRC_ModifiedDate INT64,
  datasource STRING,
  is_quarantined BOOL,
  inserted_date TIMESTAMP,
  modified_date TIMESTAMP,
  is_current BOOL
);

--- Create a quality check temp table
CREATE OR REPLACE TABLE `project-15f498fb-28c2-4528-bc7.silver_dataset.quality_checks` AS
SELECT DISTINCT
  CONCAT(SRC_PatientID, '-', datasource) AS Patient_Key,
    SRC_PatientID,
    FirstName,
    LastName,
    MiddleName,
    SSN,
    PhoneNumber,
    Gender,
    DOB,
    Address,
    ModifiedDate AS SRC_ModifiedDate,
    datasource,
    CASE
      WHEN SRC_PatientID IS NULL OR DOB IS NULL OR FirstName IS NULL OR LOWER(FirstName) = 'null' THEN TRUE
      ELSE FALSE
    END AS is_quarantined
FROM (
  SELECT DISTINCT
    PatientID AS SRC_PatientID,
    FirstName,
    LastName,
    MiddleName,
    SSN,
    PhoneNumber,
    Gender,
    DOB,
    Address,
    ModifiedDate,
    'hosa' AS datasource
  FROM `project-15f498fb-28c2-4528-bc7.bronze_dataset.patients_ha`

  UNION ALL

  SELECT DISTINCT 
    ID AS SRC_PatientID,
    F_Name as FirstName,
    L_Name as LastName,
    M_Name as MiddleName,
    SSN,
    PhoneNumber,
    Gender,
    DOB,
    Address,
    ModifiedDate,
    'hosb' AS datasource
  FROM `project-15f498fb-28c2-4528-bc7.bronze_dataset.patients_hb`
);

--- Implement SCD2 technique with MERGE
MERGE INTO `project-15f498fb-28c2-4528-bc7.silver_dataset.patients` AS target
USING `project-15f498fb-28c2-4528-bc7.silver_dataset.quality_checks` AS source
ON target.Patient_Key = source.Patient_Key
AND target.is_current = True
---- when matched the existing records needs to be marked as historical if any column values are changed
WHEN MATCHED AND (
  target.SRC_PatientID <> source.SRC_PatientID OR
  target.FirstName <> source.FirstName OR
  target.LastName <> source.LastName OR
  target.MiddleName <> source.MiddleName OR
  target.SSN <> source.SSN OR
  target.PhoneNumber <> source.PhoneNumber OR
  target.Gender <> source.Gender OR
  target.DOB <> source.DOB OR
  target.Address <> source.Address OR
  target.SRC_ModifiedDate <> source.SRC_ModifiedDate OR
  target.datasource <> source.datasource OR
  target.is_quarantined <> source.is_quarantined
)
THEN UPDATE SET 
  target.is_current = FALSE,
  target.modified_date = CURRENT_TIMESTAMP()

---- Insert new and upadated records as the latest active records when not matched
WHEN NOT MATCHED
THEN INSERT (
  Patient_Key,
  SRC_PatientID,
  FirstName,
  LastName,
  MiddleName,
  SSN,
  PhoneNumber,
  Gender,
  DOB,
  Address,
  SRC_ModifiedDate,
  datasource,
  is_quarantined,
  inserted_date,
  modified_date,
  is_current
)
VALUES (
  source.Patient_Key,
  source.SRC_PatientID,
  source.FirstName,
  source.LastName,
  source.MiddleName,
  source.SSN,
  source.PhoneNumber,
  source.Gender,
  source.DOB,
  source.Address,
  source.SRC_ModifiedDate,
  source.datasource,
  source.is_quarantined,
  CURRENT_TIMESTAMP(),  
  CURRENT_TIMESTAMP(),  
  TRUE
);

----- Drop quality_check table as we don't want it, we needed that only for that time
DROP TABLE IF EXISTS `project-15f498fb-28c2-4528-bc7.silver_dataset.quality_checks`;
