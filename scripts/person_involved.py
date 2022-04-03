from pyspark import SparkContext, Row
from pyspark.sql import SparkSession
import datetime
from pyspark.sql.functions import max, sum, first

from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()


class PersonInvolved:
    def __init__(self, spark_context=None):
        self.spark_context = spark_context

    def ethnicity_count_bodystyle(self, df_dict):
        df_unit = df_dict["Units_use"]
        df_person = df_dict["Primary_Person_use"]

        df = df_unit.join(df_person, 'CRASH_ID', 'inner').select('VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID')
        df = df.where((col('VEH_BODY_STYL_ID') != "UNKNOWN") & (col('VEH_BODY_STYL_ID') != "NA") & (col('PRSN_ETHNICITY_ID') != "NA") & (col('PRSN_ETHNICITY_ID') != "UNKNOWN"))
        df = df.groupBy('VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID').count().orderBy('VEH_BODY_STYL_ID')
        return df
