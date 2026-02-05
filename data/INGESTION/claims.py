from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, when

# Create Spark Session
spark = SparkSession.builder.appName("Healthcare Claims Ingestion").getOrCreate()

# configure variables
BUCKET_NAME = "healthcare-project"
CLAIMS_BUCKET_PATH = f"gs://{BUCKET_NAME}/landing/claims/*.csv" #source path
BQ_TABLE = "project-15f498fb-28c2-4528-bc7.bronze_dataset.claims" #destination path
TEMP_GCS_BUCKET = f"{BUCKET_NAME}/temp/"

# Read data from claims source
claims_df = spark.read.csv(CLAIMS_BUCKET_PATH, header=True)

# adding new column to get the hospital source
claims_df = (claims_df.withColumn("datasource", when(input_file_name().contains("hospital2"), "hosb")
                                 .when(input_file_name().contains("hospital1"), "hosa").otherwise("None")))

# Remove duplicates
claims_df = claims_df.dropDuplicates()

# write to Bigquery
(claims_df.write
            .format("bigquery")
            .option("table", BQ_TABLE)
            .option("temporaryGcsBucket", TEMP_GCS_BUCKET)
            .mode("overwrite")
            .save())