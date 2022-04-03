from pyspark import SparkContext, Row
from pyspark.sql import SparkSession
import datetime

from pyspark.sql.functions import col

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
