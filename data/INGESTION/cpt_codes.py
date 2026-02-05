from pyspark.sql import SparkSession

# Create Spark Session
spark = SparkSession.builder.appName("CPT Codes Ingestion").getOrCreate()

# configure variables
BUCKET_NAME = "healthcare-project"
CPT_BUCKET_PATH = f"gs://{BUCKET_NAME}/landing/cptcodes/*.csv"
BQ_TABLE = "project-15f498fb-28c2-4528-bc7.bronze_dataset.cpt_codes" #destination path
TEMP_GCS_BUCKET = f"{BUCKET_NAME}/temp/"

# Read from CPT
cptcodes_df = spark.read.csv(CPT_BUCKET_PATH, header=True)

# replace spaces in column names with underscore as bigquery doesn't support it
for col in cptcodes_df.columns:
    new_col = col.replace(" ", "_").lower()
    cptcodes_df = cptcodes_df.withColumnRenamed(col, new_col)
    
# cptcodes_df.show()

# write to bigquery
(cptcodes_df.write
            .format("bigquery")
            .option("table", BQ_TABLE)
            .option("temporaryGcsBucket", TEMP_GCS_BUCKET)
            .mode("overwrite")
            .save())