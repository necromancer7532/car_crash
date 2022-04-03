from pyspark import SparkContext, Row
from pyspark.sql import SparkSession, Window
import datetime

from pyspark.sql.functions import col, lit, row_number

spark = SparkSession.builder.getOrCreate()


class VehicleBooked:
    def __init__(self, spark_context=None):
        self.spark_context = spark_context

    def booked_vehicle(self, vehicle_type, df_dict):
        df_charges = df_dict["Charges_use"]
        df_unit = df_dict["Units_use"]

        df = df_unit.join(df_charges, 'CRASH_ID', 'inner').select('CRASH_ID', 'VIN', 'VEH_BODY_STYL_ID')
        df = df.where(col('VEH_BODY_STYL_ID') == vehicle_type)
        return df.count()

    def vehicle_model_count(self, df_dict):
        df_unit = df_dict["Units_use"]
        df_person = df_dict["Primary_Person_use"]
        df = df_unit.join(df_person, 'CRASH_ID', 'inner').select('PRSN_INJRY_SEV_ID', 'VEH_MAKE_ID')
        df = df.where((col('PRSN_INJRY_SEV_ID')=="INCAPACITATING INJURY") | (col('PRSN_INJRY_SEV_ID')=="KILLED") | (col('PRSN_INJRY_SEV_ID')=="NON-INCAPACITATING INJURY") | (col('PRSN_INJRY_SEV_ID')=="POSSIBLE INJURY"))
        df = df.groupBy("VEH_MAKE_ID").count().orderBy('count', ascending=False)
        w = Window().partitionBy(lit('a')).orderBy(lit('a'))
        df = df.withColumn("rank", row_number().over(w))
        df = df.filter(col('rank').between(5,15))
        df.show()
        return df
