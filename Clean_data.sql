-- Clean the Providers table in Silver dataset as it has headers/column names as a row
CREATE OR REPLACE TABLE `project-15f498fb-28c2-4528-bc7.silver_dataset.providers` AS
SELECT *
FROM `project-15f498fb-28c2-4528-bc7.silver_dataset.providers`
WHERE ProviderID != 'ProviderID';
