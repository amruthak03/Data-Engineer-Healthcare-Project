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

