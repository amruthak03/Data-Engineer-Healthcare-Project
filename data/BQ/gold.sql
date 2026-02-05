-- Q1. Business use case: Patient's  summary such as patient's diagnoses, insurance details, paid by them or claimed etc.
-- Creating a gold table/dataset called patient's summary so that it's easy to view

# Create a table
CREATE TABLE IF NOT EXISTS `project-15f498fb-28c2-4528-bc7.gold_dataset.patient_history` (
    Patient_Key STRING,
    FirstName STRING,
    LastName STRING,
    Gender STRING,
    DOB INT64,
    Address STRING,
    EncounterDate INT64,
    EncounterType STRING,
    Transaction_Key STRING,
    VisitDate INT64,
    VisitType STRING,
    ServiceDate INT64,
    BilledAmount FLOAT64,
    PaidAmount FLOAT64,
    ClaimStatus STRING,
    ClaimAmount STRING,
    ClaimPaidAmount STRING,
    PayorType STRING
);

# truncate table -  to quickly remove all rows from a table while keeping the table structure and its columns, constraints, and indexes intact
TRUNCATE TABLE `project-15f498fb-28c2-4528-bc7.gold_dataset.patient_history`;

# insert data into the table
INSERT INTO `project-15f498fb-28c2-4528-bc7.gold_dataset.patient_history`
SELECT 
  p.Patient_Key,
  p.FirstName,
  p.LastName,
  p.Gender,
  p.DOB,
  p.Address,
  e.EncounterDate,
  e.EncounterType,
  t.Transaction_Key,
  t.VisitDate,
  t.VisitType,
  t.ServiceDate,
  t.Amount AS BilledAmount,
  t.PaidAmount,
  c.ClaimStatus,
  c.ClaimAmount,
  c.PaidAmount AS ClaimPaidAmount,
  c.PayorType
FROM `project-15f498fb-28c2-4528-bc7.silver_dataset.patients` p
LEFT JOIN `project-15f498fb-28c2-4528-bc7.silver_dataset.encounters` e
 ON SPLIT(p.Patient_Key, '-')[OFFSET(0)] || '-' || SPLIT(p.Patient_Key, '-')[OFFSET(1)] = e.PatientID
LEFT JOIN `project-15f498fb-28c2-4528-bc7.silver_dataset.transactions` t
  ON SPLIT(p.Patient_Key, '-')[OFFSET(0)] || '-' || SPLIT(p.Patient_Key, '-')[OFFSET(1)] = t.PatientID
LEFT JOIN `project-15f498fb-28c2-4528-bc7.silver_dataset.claims` c
  ON t.SRC_TransactionID = c.TransactionID
WHERE p.is_current = TRUE;

--- Question 2. total charge amount per provider per department
# Create gold table/dataset named provider_charge_summary
CREATE TABLE IF NOT EXISTS `project-15f498fb-28c2-4528-bc7.gold_dataset.provider_charge_summary` (
  Provider_Name STRING,
  Dept_Name STRING,
  Amount FLOAT64
);

# truncate table
TRUNCATE TABLE `project-15f498fb-28c2-4528-bc7.gold_dataset.provider_charge_summary`;

# insert data into the table
INSERT INTO `project-15f498fb-28c2-4528-bc7.gold_dataset.provider_charge_summary`
SELECT 
  CONCAT(p.FirstName, ' ', p.LastName) AS Provider_Name,
  d.Name AS Dept_Name,
  SUM(t.Amount) AS Amount
FROM `project-15f498fb-28c2-4528-bc7.silver_dataset.transactions` t
LEFT JOIN `project-15f498fb-28c2-4528-bc7.silver_dataset.providers` p
  ON SPLIT(p.ProviderID, '-')[SAFE_OFFSET(1)] = t.ProviderID
LEFT JOIN `project-15f498fb-28c2-4528-bc7.silver_dataset.departments` d
  ON SPLIT(d.Dept_Id, '-')[SAFE_OFFSET(0)] = p.DeptID
WHERE t.is_quarantined = FALSE AND d.Name IS NOT NULL
GROUP BY Provider_Name, Dept_Name;

--- Business Question 3: Provider's performance summary - summarizing provider's activity, including the no. of encounters with patients, total billed amount, and claim success rate. 
-- Create gold table/dataset called provider_performance
CREATE TABLE IF NOT EXISTS `project-15f498fb-28c2-4528-bc7.gold_dataset.provider_performance` (
    ProviderID STRING,
    FirstName STRING,
    LastName STRING,
    Specialization STRING,
    TotalEncounters INT64,
    TotalTransactions INT64,
    TotalBilledAmount FLOAT64,
    TotalPaidAmount FLOAT64,
    ApprovedClaims INT64,
    TotalClaims INT64,
    ClaimApprovalRate FLOAT64
);

# truncate table
TRUNCATE TABLE `project-15f498fb-28c2-4528-bc7.gold_dataset.provider_performance`;

# insert data into table
INSERT INTO `project-15f498fb-28c2-4528-bc7.gold_dataset.provider_performance`
SELECT 
  pr.ProviderID,
  pr.FirstName,
  pr.LastName,
  pr.Specialization,
  COUNT(DISTINCT e.Encounter_Key) AS TotalEncounters,
  COUNT(DISTINCT t.Transaction_Key) AS TotalTransactions,
  SUM(t.Amount) AS TotalBilledAmount,
  SUM(t.PaidAmount) AS TotalPaidAmount,
  COUNT(DISTINCT CASE WHEN c.ClaimStatus = 'Approved' THEN c.Claim_Key END) AS ApprovedClaims,
  COUNT(DISTINCT c.Claim_Key) AS TotalClaims,
  ROUND(
    (COUNT(DISTINCT CASE WHEN c.ClaimStatus = 'Approved' THEN c.Claim_Key END) / NULLIF(COUNT(DISTINCT c.Claim_Key),0)) * 100, 2
    ) AS ClaimApprovalRate
FROM `project-15f498fb-28c2-4528-bc7.silver_dataset.providers` pr
LEFT JOIN `project-15f498fb-28c2-4528-bc7.silver_dataset.encounters` e
 ON SPLIT(pr.ProviderID, '-')[SAFE_OFFSET(1)] = e.ProviderID
LEFT JOIN `project-15f498fb-28c2-4528-bc7.silver_dataset.transactions` t
 ON SPLIT(pr.ProviderID, "-")[SAFE_OFFSET(1)] = t.ProviderID
LEFT JOIN `project-15f498fb-28c2-4528-bc7.silver_dataset.claims` c
 ON t.SRC_TransactionID = c.TransactionID
GROUP BY pr.ProviderID, pr.FirstName, pr.LastName, pr.Specialization;

--- Business Question 4: Department's performance analytics - provides insights into department level efficiency, revenue, and patient volume.
# create table/gold dataset
CREATE TABLE IF NOT EXISTS `project-15f498fb-28c2-4528-bc7.gold_dataset.department_performance` (
    Dept_Id STRING,
    DepartmentName STRING,
    TotalEncounters INT64,
    TotalTransactions INT64,
    TotalBilledAmount FLOAT64,
    TotalPaidAmount FLOAT64,
    AvgPaymentPerTransaction FLOAT64
);

# truncate table
TRUNCATE TABLE `project-15f498fb-28c2-4528-bc7.gold_dataset.department_performance`;

# insert into table
INSERT INTO `project-15f498fb-28c2-4528-bc7.gold_dataset.department_performance`
SELECT 
 d.Dept_Id,
 d.Name AS DepartmentName,
 COUNT(DISTINCT e.Encounter_Key) AS TotalEncounters,
 COUNT(DISTINCT t.Transaction_Key) AS TotalTransactions,
 SUM(t.Amount) AS TotalBilledAmount,
 SUM(t.PaidAmount) AS TotalPaidAmount,
 AVG(t.PaidAmount) AS AvgPaymentPerTransaction
FROM `project-15f498fb-28c2-4528-bc7.silver_dataset.departments` AS d
LEFT JOIN `project-15f498fb-28c2-4528-bc7.silver_dataset.encounters` AS e 
 ON SPLIT(d.Dept_Id, '-')[SAFE_OFFSET(0)] = e.DepartmentID
LEFT JOIN `project-15f498fb-28c2-4528-bc7.silver_dataset.transactions` AS t
  ON SPLIT(d.Dept_Id, '-')[SAFE_OFFSET(0)] = t.DeptID
WHERE d.is_quarantined = FALSE
GROUP BY d.Dept_Id, d.Name;

--- Business Question 5: computing financial metrics (KPIs). 
# create gold table
CREATE TABLE IF NOT EXISTS `project-15f498fb-28c2-4528-bc7.gold_dataset.financial_metrics` (
  TotalTransactions INT64,
  TotalClaims INT64,
  ApprovedClaims INT64,
  RejectedClaims INT64,
  ClaimSuccesRate FLOAT64,
  TotalBilledAmount FLOAT64,
  TotalPaidAmount FLOAT64,
  OutstandingBalances FLOAT64,
  TotalEncounters INT64,
  TotalPatients INT64
);

# truncate table
TRUNCATE TABLE `project-15f498fb-28c2-4528-bc7.gold_dataset.financial_metrics`;

#insert into table
INSERT INTO `project-15f498fb-28c2-4528-bc7.gold_dataset.financial_metrics`
SELECT 
  COUNT(DISTINCT t.Transaction_Key) AS TotalTransactions,
  COUNT(DISTINCT c.Claim_Key) AS TotalClaims,
  COUNT(DISTINCT CASE WHEN c.ClaimStatus = 'Approved' THEN c.Claim_Key END) AS ApprovedClaims,
  COUNT(DISTINCT CASE WHEN c.ClaimStatus = 'Rejected' THEN c.Claim_Key END) AS RejectedClaims,
  ROUND(
    COUNT(DISTINCT CASE WHEN c.ClaimStatus = 'Approved' THEN c.Claim_Key END) / NULLIF(COUNT(DISTINCT c.Claim_Key), 0) * 100, 2
  ) AS ClaimSuccessRate,
  ROUND(SUM(t.Amount), 2) AS TotalBilledAmount,
  ROUND(SUM(t.PaidAmount), 2) AS TotalPaidAmount,
  ROUND(SUM(t.Amount) - SUM(t.PaidAmount), 2) AS OutstandingBalances,
  COUNT(DISTINCT e.Encounter_Key) AS TotalEncounters,
  COUNT(DISTINCT p.Patient_Key) AS TotalPatients
FROM `project-15f498fb-28c2-4528-bc7.silver_dataset.transactions` AS t
LEFT JOIN `project-15f498fb-28c2-4528-bc7.silver_dataset.claims` AS c
 ON SPLIT(t.Transaction_Key, '-')[SAFE_OFFSET(0)] = c.TransactionID
LEFT JOIN `project-15f498fb-28c2-4528-bc7.silver_dataset.encounters` AS e
 ON SPLIT(e.Encounter_Key, '-')[SAFE_OFFSET(0)] = t.EncounterID
LEFT JOIN `project-15f498fb-28c2-4528-bc7.silver_dataset.patients` AS p
 ON SPLIT(p.Patient_Key, '-')[OFFSET(0)] || '-' || SPLIT(p.Patient_Key, '-')[OFFSET(1)] = t.PatientID
 WHERE t.is_current = TRUE AND (c.is_current = TRUE OR c.is_current IS NULL);


---- Business Question 6b - financial metrics by month
# create gold table
CREATE TABLE IF NOT EXISTS `project-15f498fb-28c2-4528-bc7.gold_dataset.financial_metrics_bymonth` (
  ServiceMonth STRING,
  TotalTransactions INT64,
  TotalClaims INT64,
  ApprovedClaims INT64,
  ClaimSuccesRate FLOAT64,
  TotalBilledAmount FLOAT64,
  TotalPaidAmount FLOAT64,
  OutstandingBalances FLOAT64
);

# truncate table
TRUNCATE TABLE `project-15f498fb-28c2-4528-bc7.gold_dataset.financial_metrics_bymonth`;

# insert into
INSERT INTO `project-15f498fb-28c2-4528-bc7.gold_dataset.financial_metrics_bymonth`
SELECT 
  FORMAT_DATE('%Y-%m', DATE(TIMESTAMP_MILLIS(t.ServiceDate))) AS ServiceMonth,
  COUNT(DISTINCT t.Transaction_Key) AS TotalTransactions,
  COUNT(DISTINCT c.Claim_Key) AS TotalClaims,
  COUNT(DISTINCT CASE WHEN c.ClaimStatus = 'Approved' THEN c.Claim_Key END) AS ApprovedClaims,
  ROUND(
    COUNT(DISTINCT CASE WHEN c.ClaimStatus = 'Approved' THEN c.Claim_Key END) / NULLIF(COUNT(DISTINCT c.Claim_Key), 0) * 100, 2
  ) AS ClaimSuccessRate,
  ROUND(SUM(t.Amount), 2) AS TotalBilledAmount,
  ROUND(SUM(t.PaidAmount), 2) AS TotalPaidAmount,
  ROUND(SUM(t.Amount) - SUM(t.PaidAmount), 2) AS OutstandingBalances
FROM `project-15f498fb-28c2-4528-bc7.silver_dataset.transactions` AS t
LEFT JOIN `project-15f498fb-28c2-4528-bc7.silver_dataset.claims` AS c
 ON SPLIT(t.Transaction_Key, '-')[SAFE_OFFSET(0)] = c.TransactionID
 WHERE t.is_current = TRUE AND (c.is_current = TRUE OR c.is_current IS NULL)
 GROUP BY ServiceMonth
 ORDER BY ServiceMonth;

 --- Business Question 7: Payor Performance and Claims Summary
 # create gold table
CREATE TABLE IF NOT EXISTS `project-15f498fb-28c2-4528-bc7.gold_dataset.payor_performance` (
  PayorID STRING,
  PayorType STRING,
  TotalClaims INT64,
  ApprovedClaims INT64,
  ClaimApprovedRate FLOAT64,
  TotalClaimAmount FLOAT64,
  TotalPaidAmount FLOAT64,
  AvgPaidPerClaim FLOAT64,
  AvgClaimProcessingDays FLOAT64
);

# truncate table
TRUNCATE TABLE `project-15f498fb-28c2-4528-bc7.gold_dataset.payor_performance`;

INSERT INTO `project-15f498fb-28c2-4528-bc7.gold_dataset.payor_performance`
 SELECT 
  c.PayorID,
  c.PayorType,
  COUNT(DISTINCT c.Claim_Key) AS TotalClaims,
  COUNT(DISTINCT CASE WHEN c.ClaimStatus = 'Approved' THEN c.Claim_Key END) AS ApprovedClaims,
  ROUND(
    COUNT(DISTINCT CASE WHEN c.ClaimStatus = 'Approved' THEN c.Claim_Key END) / NULLIF(COUNT(DISTINCT c.Claim_Key), 0) * 100, 2
  ) AS ClaimApprovedRate,
  ROUND(SUM(SAFE_CAST(c.ClaimAmount AS NUMERIC)), 2) AS TotalClaimAmount,
  ROUND(SUM(SAFE_CAST(c.PaidAmount AS NUMERIC)), 2) AS TotalPaidAmount,
  ROUND(
    SUM(SAFE_CAST(c.PaidAmount AS NUMERIC)) / NULLIF(COUNT(DISTINCT c.Claim_Key), 0),
    2
  ) AS AvgPaidPerClaim,
  ROUND(
    AVG(
      DATE_DIFF(DATE(c.ClaimDate), DATE(c.ServiceDate), DAY)
    ), 2
  ) AS AvgClaimProcessingDays
FROM `project-15f498fb-28c2-4528-bc7.silver_dataset.claims` AS c
WHERE c.is_current = TRUE
GROUP BY c.PayorID, c.PayorType
ORDER BY ClaimApprovedRate DESC;





