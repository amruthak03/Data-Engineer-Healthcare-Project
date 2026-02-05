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
