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

