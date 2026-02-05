-- Here, will implement both SCD2 and CDM techniques for creating tables in silver layer

-- Create departments table (which is a full load) by merging data from hospital A and B
CREATE TABLE IF NOT EXISTS `project-15f498fb-28c2-4528-bc7.silver_dataset.departments` (
  Dept_Id STRING,
  SRC_Dept_Id STRING,
  Name STRING,
  datasource STRING,
  is_quarantined BOOLEAN
);

-- If the table already exists in the silver layer, then we truncate the table nefore inserting.
TRUNCATE TABLE `project-15f498fb-28c2-4528-bc7.silver_dataset.departments`;

-- full load by inserting merged data
INSERT INTO `project-15f498fb-28c2-4528-bc7.silver_dataset.departments`
SELECT DISTINCT
      CONCAT(deptid, '-', datasource) AS Dept_Id,
      deptid AS SRC_Dept_Id,
      Name,
      datasource,
      CASE
        WHEN deptid IS NULL OR Name IS NULL THEN TRUE
        ELSE FALSE
      END AS is_quarantined
FROM (
  SELECT DISTINCT *, 'hosa' AS datasource FROM `project-15f498fb-28c2-4528-bc7.bronze_dataset.departments_ha`
  UNION ALL
  SELECT DISTINCT *, 'hosb' AS datasource FROM `project-15f498fb-28c2-4528-bc7.bronze_dataset.departments_hb`
);
--- End of departments table full load

--- Here, we will create table 'providers' which is a full load data by merging the data from both hospital A and B
CREATE TABLE IF NOT EXISTS `project-15f498fb-28c2-4528-bc7.silver_dataset.providers` (
    ProviderID STRING,
    FirstName STRING,
    LastName STRING,
    Specialization STRING,
    DeptID STRING,
    NPI INT64,
    datasource STRING,
    is_quarantined BOOLEAN
);

--- If the table already exists in the silver layer, then we truncate the table nefore inserting.
 TRUNCATE TABLE `project-15f498fb-28c2-4528-bc7.silver_dataset.providers`;

 --- full load by inserting merged data
 INSERT INTO `project-15f498fb-28c2-4528-bc7.silver_dataset.providers`
 SELECT DISTINCT
    ProviderID,
    FirstName,
    LastName,
    Specialization,
    DeptID,
    CAST(NPI AS INT64) AS NPI,
    datasource,
    CASE
      WHEN ProviderID IS NULL OR DeptID IS NULL THEN TRUE 
      ELSE FALSE
    END AS is_quarantined
FROM (
  SELECT DISTINCT *, 'hosa' AS datasource FROM `project-15f498fb-28c2-4528-bc7.bronze_dataset.providers_ha`
  UNION ALL 
  SELECT DISTINCT *, 'hosb' AS datasource FROM `project-15f498fb-28c2-4528-bc7.bronze_dataset.providers_hb`
);
--- End of providers table full load

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
--- End of patient table SCD2 implementation

--- Create Transactions table in silver layer. It is an incremantal table
CREATE TABLE IF NOT EXISTS `project-15f498fb-28c2-4528-bc7.silver_dataset.transactions` (
    Transaction_Key STRING,
    SRC_TransactionID STRING,
    EncounterID STRING,
    PatientID STRING,
    ProviderID STRING,
    DeptID STRING,
    VisitDate INT64,
    ServiceDate INT64,
    PaidDate INT64,
    VisitType STRING,
    Amount FLOAT64,
    AmountType STRING,
    PaidAmount FLOAT64,
    ClaimID STRING,
    PayorID STRING,
    ProcedureCode INT64,
    ICDCode STRING,
    LineOfBusiness STRING,
    MedicaidID STRING,
    MedicareID STRING,
    SRC_InsertDate INT64,
    SRC_ModifiedDate INT64,
    datasource STRING,
    is_quarantined BOOL,
    inserted_date TIMESTAMP,
    modified_date TIMESTAMP,
    is_current BOOL
);

---- Create a quality checks temp table
CREATE OR REPLACE TABLE `project-15f498fb-28c2-4528-bc7.silver_dataset.quality_checks` AS
SELECT DISTINCT
    CONCAT(TransactionID, '-', datasource) AS Transaction_Key,
    TransactionID AS SRC_TransactionID,
    EncounterID,
    PatientID,
    ProviderID,
    DeptID,
    VisitDate,
    ServiceDate,
    PaidDate,
    VisitType,
    Amount,
    AmountType,
    PaidAmount,
    ClaimID,
    PayorID,
    ProcedureCode,
    ICDCode,
    LineOfBusiness,
    MedicaidID,
    MedicareID,
    InsertDate AS SRC_InsertDate,
    ModifiedDate AS SRC_ModifiedDate,
    datasource,
    CASE 
        WHEN EncounterID IS NULL OR PatientID IS NULL OR TransactionID IS NULL OR VisitDate IS NULL THEN TRUE
        ELSE FALSE
    END AS is_quarantined
FROM (
    SELECT DISTINCT *, 'hosa' AS datasource FROM `project-15f498fb-28c2-4528-bc7.bronze_dataset.transactions_ha`
    UNION ALL
    SELECT DISTINCT *, 'hosb' AS datasource FROM `project-15f498fb-28c2-4528-bc7.bronze_dataset.transactions_hb`
);

--- Implement SCD2 technique with MERGE
MERGE INTO `project-15f498fb-28c2-4528-bc7.silver_dataset.transactions` AS target
USING `project-15f498fb-28c2-4528-bc7.silver_dataset.quality_checks` AS source
ON target.Transaction_Key = source.Transaction_Key
AND target.is_current = TRUE 

--- step 1: when matched the existing records needs to be marked as historical if any column values are changed
WHEN MATCHED AND (
    target.SRC_TransactionID <> source.SRC_TransactionID OR
    target.EncounterID <> source.EncounterID OR
    target.PatientID <> source.PatientID OR
    target.ProviderID <> source.ProviderID OR
    target.DeptID <> source.DeptID OR
    target.VisitDate <> source.VisitDate OR
    target.ServiceDate <> source.ServiceDate OR
    target.PaidDate <> source.PaidDate OR
    target.VisitType <> source.VisitType OR
    target.Amount <> source.Amount OR
    target.AmountType <> source.AmountType OR
    target.PaidAmount <> source.PaidAmount OR
    target.ClaimID <> source.ClaimID OR
    target.PayorID <> source.PayorID OR
    target.ProcedureCode <> source.ProcedureCode OR
    target.ICDCode <> source.ICDCode OR
    target.LineOfBusiness <> source.LineOfBusiness OR
    target.MedicaidID <> source.MedicaidID OR
    target.MedicareID <> source.MedicareID OR
    target.SRC_InsertDate <> source.SRC_InsertDate OR
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
    Transaction_Key,
    SRC_TransactionID,
    EncounterID,
    PatientID,
    ProviderID,
    DeptID,
    VisitDate,
    ServiceDate,
    PaidDate,
    VisitType,
    Amount,
    AmountType,
    PaidAmount,
    ClaimID,
    PayorID,
    ProcedureCode,
    ICDCode,
    LineOfBusiness,
    MedicaidID,
    MedicareID,
    SRC_InsertDate,
    SRC_ModifiedDate,
    datasource,
    is_quarantined,
    inserted_date,
    modified_date,
    is_current
)
VALUES (
    source.Transaction_Key,
    source.SRC_TransactionID,
    source.EncounterID,
    source.PatientID,
    source.ProviderID,
    source.DeptID,
    source.VisitDate,
    source.ServiceDate,
    source.PaidDate,
    source.VisitType,
    source.Amount,
    source.AmountType,
    source.PaidAmount,
    source.ClaimID,
    source.PayorID,
    source.ProcedureCode,
    source.ICDCode,
    source.LineOfBusiness,
    source.MedicaidID,
    source.MedicareID,
    source.SRC_InsertDate,
    source.SRC_ModifiedDate,
    source.datasource,
    source.is_quarantined,
    CURRENT_TIMESTAMP(),  
    CURRENT_TIMESTAMP(),  
    TRUE 
);

----- Drop quality_check table as we don't want it, we needed that only for that time
DROP TABLE IF EXISTS `project-15f498fb-28c2-4528-bc7.silver_dataset.quality_checks`;
--- End of transactions table SCD2 implementation

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
--- End of encounters table SCD2 implementation

--- Create Claims table
CREATE TABLE IF NOT EXISTS `project-15f498fb-28c2-4528-bc7.silver_dataset.claims` (
    Claim_Key STRING,
    SRC_ClaimID STRING,
    TransactionID STRING,
    PatientID STRING,
    EncounterID STRING,
    ProviderID STRING,
    DeptID STRING,
    ServiceDate STRING,
    ClaimDate STRING,
    PayorID STRING,
    ClaimAmount STRING,
    PaidAmount STRING,
    ClaimStatus STRING,
    PayorType STRING,
    Deductible STRING,
    Coinsurance STRING,
    Copay STRING,
    SRC_InsertDate STRING,
    SRC_ModifiedDate STRING,
    datasource STRING,
    is_quarantined BOOLEAN,
    inserted_date TIMESTAMP,
    modified_date TIMESTAMP,
    is_current BOOLEAN
);

-- 2. Create a quality_checks temp table for claims
CREATE OR REPLACE TABLE `project-15f498fb-28c2-4528-bc7.silver_dataset.quality_checks_claims` AS
SELECT 
    CONCAT(SRC_ClaimID, '-', datasource) AS Claim_Key,
    SRC_ClaimID,
    TransactionID,
    PatientID,
    EncounterID,
    ProviderID,
    DeptID,
    ServiceDate,
    ClaimDate,
    PayorID,
    ClaimAmount,
    PaidAmount,
    ClaimStatus,
    PayorType,
    Deductible,
    Coinsurance,
    Copay,
    InsertDate AS SRC_InsertDate,
    ModifiedDate AS SRC_ModifiedDate,
    datasource,
    CASE 
        WHEN SRC_ClaimID IS NULL OR PatientID IS NULL OR TransactionID IS NULL OR LOWER(ClaimStatus) = 'null' THEN TRUE
        ELSE FALSE
    END AS is_quarantined
FROM (
    SELECT 
        ClaimID AS SRC_ClaimID,
        TransactionID,
        PatientID,
        EncounterID,
        ProviderID,
        DeptID,
        ServiceDate,
        ClaimDate,
        PayorID,
        ClaimAmount,
        PaidAmount,
        ClaimStatus,
        PayorType,
        Deductible,
        Coinsurance,
        Copay,
        InsertDate,
        ModifiedDate,
        'hosa' AS datasource
    FROM `project-15f498fb-28c2-4528-bc7.bronze_dataset.claims`
);

-- 3. Apply SCD Type 2 Logic with MERGE
MERGE INTO `project-15f498fb-28c2-4528-bc7.silver_dataset.claims` AS target
USING `project-15f498fb-28c2-4528-bc7.silver_dataset.quality_checks_claims` AS source
ON target.Claim_Key = source.Claim_Key
AND target.is_current = TRUE 

-- Step 1: Mark existing records as historical if any column has changed
WHEN MATCHED AND (
    target.SRC_ClaimID <> source.SRC_ClaimID OR
    target.TransactionID <> source.TransactionID OR
    target.PatientID <> source.PatientID OR
    target.EncounterID <> source.EncounterID OR
    target.ProviderID <> source.ProviderID OR
    target.DeptID <> source.DeptID OR
    target.ServiceDate <> source.ServiceDate OR
    target.ClaimDate <> source.ClaimDate OR
    target.PayorID <> source.PayorID OR
    target.ClaimAmount <> source.ClaimAmount OR
    target.PaidAmount <> source.PaidAmount OR
    target.ClaimStatus <> source.ClaimStatus OR
    target.PayorType <> source.PayorType OR
    target.Deductible <> source.Deductible OR
    target.Coinsurance <> source.Coinsurance OR
    target.Copay <> source.Copay OR
    target.SRC_ModifiedDate <> source.SRC_ModifiedDate OR
    target.datasource <> source.datasource OR
    target.is_quarantined <> source.is_quarantined
)
THEN UPDATE SET 
    target.is_current = FALSE,
    target.modified_date = CURRENT_TIMESTAMP()

-- Step 2: Insert new and updated records as the latest active records
WHEN NOT MATCHED 
THEN INSERT (
    Claim_Key,
    SRC_ClaimID,
    TransactionID,
    PatientID,
    EncounterID,
    ProviderID,
    DeptID,
    ServiceDate,
    ClaimDate,
    PayorID,
    ClaimAmount,
    PaidAmount,
    ClaimStatus,
    PayorType,
    Deductible,
    Coinsurance,
    Copay,
    SRC_InsertDate,
    SRC_ModifiedDate,
    datasource,
    is_quarantined,
    inserted_date,
    modified_date,
    is_current
)
VALUES (
    source.Claim_Key,
    source.SRC_ClaimID,
    source.TransactionID,
    source.PatientID,
    source.EncounterID,
    source.ProviderID,
    source.DeptID,
    source.ServiceDate,
    source.ClaimDate,
    source.PayorID,
    source.ClaimAmount,
    source.PaidAmount,
    source.ClaimStatus,
    source.PayorType,
    source.Deductible,
    source.Coinsurance,
    source.Copay,
    source.SRC_InsertDate,
    source.SRC_ModifiedDate,
    source.datasource,
    source.is_quarantined,
    CURRENT_TIMESTAMP(),  
    CURRENT_TIMESTAMP(),  
    TRUE 
);

-- 4. DROP quality_check table
DROP TABLE IF EXISTS `project-15f498fb-28c2-4528-bc7.silver_dataset.quality_checks_claims`;
--- End of claims table SCD2 implementation

--- Creating CP codes table in the silver dataset
CREATE TABLE IF NOT EXISTS `project-15f498fb-28c2-4528-bc7.silver_dataset.cpt_codes` (
    CP_Code_Key STRING,
    procedure_code_category STRING,
    cpt_codes STRING,
    procedure_code_descriptions STRING,
    code_status STRING,
    datasource STRING,
    is_quarantined BOOLEAN,
    inserted_date TIMESTAMP,
    modified_date TIMESTAMP,
    is_current BOOLEAN
);

-- 2. Create a quality_checks temp table for CP Codes
CREATE OR REPLACE TABLE `project-15f498fb-28c2-4528-bc7.silver_dataset.quality_checks_cpt_codes` AS
SELECT 
    CONCAT(cpt_codes, '-', datasource) AS CP_Code_Key,
    procedure_code_category,
    cpt_codes,
    procedure_code_descriptions,
    code_status,
    datasource,
    -- Define a quarantine condition (null values in key fields)
    CASE 
        WHEN cpt_codes IS NULL OR LOWER(code_status) = 'null' THEN TRUE
        ELSE FALSE
    END AS is_quarantined
FROM (
    SELECT 
        procedure_code_category,
        cpt_codes,
        procedure_code_descriptions,
        code_status,
        'hosa' AS datasource
    FROM `project-15f498fb-28c2-4528-bc7.bronze_dataset.cpt_codes`
);

-- 3. Apply SCD Type 2 Logic with MERGE
MERGE INTO `project-15f498fb-28c2-4528-bc7.silver_dataset.cpt_codes` AS target
USING `project-15f498fb-28c2-4528-bc7.silver_dataset.quality_checks_cpt_codes` AS source
ON target.CP_Code_Key = source.CP_Code_Key
AND target.is_current = TRUE 

-- Step 1: Mark existing records as historical if any column has changed
WHEN MATCHED AND (
    target.procedure_code_category <> source.procedure_code_category OR
    target.cpt_codes <> source.cpt_codes OR
    target.procedure_code_descriptions <> source.procedure_code_descriptions OR
    target.code_status <> source.code_status OR
    target.datasource <> source.datasource OR
    target.is_quarantined <> source.is_quarantined
)
THEN UPDATE SET 
    target.is_current = FALSE,
    target.modified_date = CURRENT_TIMESTAMP()

-- Step 2: Insert new and updated records as the latest active records
WHEN NOT MATCHED 
THEN INSERT (
    CP_Code_Key,
    procedure_code_category,
    cpt_codes,
    procedure_code_descriptions,
    code_status,
    datasource,
    is_quarantined,
    inserted_date,
    modified_date,
    is_current
)
VALUES (
    source.CP_Code_Key,
    source.procedure_code_category,
    source.cpt_codes,
    source.procedure_code_descriptions,
    source.code_status,
    source.datasource,
    source.is_quarantined,
    CURRENT_TIMESTAMP(),  
    CURRENT_TIMESTAMP(),  
    TRUE 
);

-- 4. DROP quality_check table
DROP TABLE IF EXISTS `project-15f498fb-28c2-4528-bc7.silver_dataset.quality_checks_cpt_codes`;

--- End of CP codes table SCD2 implementation
