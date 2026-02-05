-- Create external tables for bronze dataset in BigQuery

-- departments table
CREATE EXTERNAL TABLE IF NOT EXISTS `project-15f498fb-28c2-4528-bc7.bronze_dataset.departments_ha`
OPTIONS(
  format = "JSON",
  uris = ['gs://healthcare-project/landing/hospital-a/departments/*.json']
);

-- encounters table
CREATE EXTERNAL TABLE IF NOT EXISTS `project-15f498fb-28c2-4528-bc7.bronze_dataset.encounters_ha`
OPTIONS(
  format = "JSON",
  uris = ['gs://healthcare-project/landing/hospital-a/encounters/*.json']
);

-- patients table
CREATE EXTERNAL TABLE IF NOT EXISTS `project-15f498fb-28c2-4528-bc7.bronze_dataset.patients_ha`
OPTIONS(
  format = "JSON",
  uris = ['gs://healthcare-project/landing/hospital-a/patients/*.json']
);

-- providers table
CREATE EXTERNAL TABLE IF NOT EXISTS `project-15f498fb-28c2-4528-bc7.bronze_dataset.providers_ha`
OPTIONS(
  format = "JSON",
  uris = ['gs://healthcare-project/landing/hospital-a/providers/*.json']
);

-- transactions table
CREATE EXTERNAL TABLE IF NOT EXISTS `project-15f498fb-28c2-4528-bc7.bronze_dataset.transactions_ha`
OPTIONS(
  format = "JSON",
  uris = ['gs://healthcare-project/landing/hospital-a/transactions/*.json']
);

----- hospital b
-- departments table
CREATE EXTERNAL TABLE IF NOT EXISTS `project-15f498fb-28c2-4528-bc7.bronze_dataset.departments_hb`
OPTIONS(
  format = "JSON",
  uris = ['gs://healthcare-project/landing/hospital-b/departments/*.json']
);

-- encounters table
CREATE EXTERNAL TABLE IF NOT EXISTS `project-15f498fb-28c2-4528-bc7.bronze_dataset.encounters_hb`
OPTIONS(
  format = "JSON",
  uris = ['gs://healthcare-project/landing/hospital-b/encounters/*.json']
);

-- patients table
CREATE EXTERNAL TABLE IF NOT EXISTS `project-15f498fb-28c2-4528-bc7.bronze_dataset.patients_hb`
OPTIONS(
  format = "JSON",
  uris = ['gs://healthcare-project/landing/hospital-b/patients/*.json']
);

-- providers table
CREATE EXTERNAL TABLE IF NOT EXISTS `project-15f498fb-28c2-4528-bc7.bronze_dataset.providers_hb`
OPTIONS(
  format = "JSON",
  uris = ['gs://healthcare-project/landing/hospital-b/providers/*.json']
);

-- transactions table
CREATE EXTERNAL TABLE IF NOT EXISTS `project-15f498fb-28c2-4528-bc7.bronze_dataset.transactions_hb`
OPTIONS(
  format = "JSON",
  uris = ['gs://healthcare-project/landing/hospital-b/transactions/*.json']
);
