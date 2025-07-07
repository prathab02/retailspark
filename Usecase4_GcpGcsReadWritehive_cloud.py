from pyspark.sql.functions import *
from pyspark.sql.types import *
def main():
   from pyspark.sql import SparkSession
   # define spark configuration object
   spark = SparkSession.builder.appName("GCP GCS Hive Read/Write").enableHiveSupport().getOrCreate()
   print("Use Spark Application to Read csv data from cloud GCS+Hive and get a DF created with the GCS data in the GCP, convert csv to json in the DF and store the json into new cloud GCS+Hive location")
   #print("Hive to GCS to hive starts here")
   custstructtype1 = StructType([StructField("id", IntegerType(), False),
                              StructField("custfname", StringType(), False),
                              StructField("custlname", StringType(), True),
                              StructField("custage", ShortType(), True),
                              StructField("custprofession", StringType(), True)])
   gcs_df = spark.read.csv("gs://prathab-wd34/Inputs/custs",mode='dropmalformed',schema=custstructtype1)
   gcs_df.show(10)
   print("GCS Read Completed Successfully")
   gcs_df.write.mode("overwrite").partitionBy("custage").saveAsTable("default.cust_info_gcs")
   print("GCS to hive table load Completed Successfully")

   print("Hive to GCS usecase starts here")
   gcs_df=spark.read.table("default.cust_info_gcs")
   curts = spark.createDataFrame([1], IntegerType()).withColumn("curts", current_timestamp()).select(date_format(col("curts"), "yyyyMMddHHmmSS")).first()[0]
   gcs_df.repartition(2).write.json("gs://prathab-wd34/output/dataset/cust_output_json_"+curts)
   print("gcs Write Completed Successfully")

   print("Hive to GCS usecase starts here")
   gcs_df=spark.read.table("default.cust_info_gcs")
   curts = spark.createDataFrame([1], IntegerType()).withColumn("curts", current_timestamp()).select(date_format(col("curts"), "yyyyMMddHHmmSS")).first()[0]
   print(curts)
   gcs_df.repartition(2).write.mode("overwrite").csv("gs://prathab-wd34/output/dataset/cust_csv")
   print("gcs Write Completed Successfully")
main()
