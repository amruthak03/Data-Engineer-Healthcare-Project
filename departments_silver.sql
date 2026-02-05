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