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