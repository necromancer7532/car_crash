from pyspark import SparkContext, Row
from pyspark.sql import SparkSession
import datetime

from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

class AccidentCount:
    def __init__(self, output_path, spark_context = None):
        self.spark_context = spark_context
        self.output_path = output_path

    def count_accidents(self, person_gender, person_status, df_dict):
        df = df_dict["Primary_Person_use"]
        df = df.where((col("PRSN_INJRY_SEV_ID") == person_status) & (col("PRSN_GNDR_ID") == person_gender))
        df = df.select("CRASH_ID").distinct()
        count = df.count()
        df_write = self.spark_context.createDataFrame([Row(count)], schema='No_of_accidents_male_deaths double')
        df_write.coalesce(1).write.option("header", "true").csv(self.output_path + "\\analysis_1.csv")




