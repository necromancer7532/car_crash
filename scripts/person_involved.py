from pyspark import SparkContext, Row
from pyspark.sql import SparkSession, Window
import datetime
from pyspark.sql.functions import max, sum, first, row_number, lit

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
        df = df.groupBy('VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID').count().orderBy('VEH_BODY_STYL_ID', 'count', ascending=False)
        w = Window().partitionBy("VEH_BODY_STYL_ID").orderBy(col('count').desc())
        df = df.withColumn("rank", row_number().over(w))
        df = df.where(col('rank') == 1).drop('rank')
        return df
    def zip_code_crashes(self, df_dict):
        df_unit = df_dict["Units_use"]
        df_person = df_dict["Primary_Person_use"]
        df = df_unit.join(df_person, 'CRASH_ID', 'inner').select('VEH_BODY_STYL_ID', 'CONTRIB_FACTR_1_ID', 'DRVR_ZIP')
        df = df.where((col('VEH_BODY_STYL_ID') == "PASSENGER CAR, 4-DOOR") | (col('VEH_BODY_STYL_ID') == "PASSENGER CAR, 2-DOOR") | (col('VEH_BODY_STYL_ID') == "POLICE CAR/TRUCK") )
        df = df.where((col('CONTRIB_FACTR_1_ID') == "UNDER INFLUENCE - ALCOHOL") | (col('CONTRIB_FACTR_1_ID') == "HAD BEEN DRINKING") )
        df = df.groupBy('DRVR_ZIP').count().orderBy(col('count').desc())
        w = Window().partitionBy(lit('a')).orderBy(lit('a'))
        df = df.withColumn("rank", row_number().over(w))
        df = df.where(col('rank') <= 5).drop('rank')
        return df


