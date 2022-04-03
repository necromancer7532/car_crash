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
        df = df.filter(col('rank').between(5,15)).drop('rank')
        return df
    def vehicle_make(self,df_dict):
        df_unit = df_dict["Units_use"]
        df_person = df_dict["Primary_Person_use"]
        df_charges = df_dict["Charges_use"]

        df = df_unit.join(df_person, 'CRASH_ID', 'inner').select('VEH_MAKE_ID', 'CONTRIB_FACTR_1_ID', 'CONTRIB_FACTR_2_ID', 'DRVR_LIC_CLS_ID', 'VEH_COLOR_ID', 'VEH_LIC_STATE_ID')

        #find top 25 offence states
        df_offense_states = df_charges.join(df_unit, 'CRASH_ID','inner').select('VEH_LIC_STATE_ID','CRASH_ID')
        df_offense_states = df_offense_states.groupBy('VEH_LIC_STATE_ID').count().orderBy('count', ascending=False)
        w = Window().partitionBy(lit('a')).orderBy(lit('a'))
        df_offense_states = df_offense_states.withColumn("rank", row_number().over(w))
        df_offense_states = df_offense_states.where(col('rank') <= 25).drop('rank')

        #top 10 used vehicle colours
        df_colours = df_unit.select('VEH_COLOR_ID', 'CRASH_ID')
        df_colours = df_colours.groupBy('VEH_COLOR_ID').count().orderBy('count', ascending=False)
        w = Window().partitionBy(lit('a')).orderBy(lit('a'))
        df_colours = df_colours.withColumn("rank", row_number().over(w))
        df_colours = df_colours.where(col('rank') <= 10).drop('rank')

        df = df.join(df_offense_states, 'VEH_LIC_STATE_ID', 'inner') \
                .join(df_colours, 'VEH_COLOR_ID', 'inner') \
                .where((col('CONTRIB_FACTR_1_ID').contains("SPEED")) | (col('CONTRIB_FACTR_2_ID').contains("SPEED"))) \
                .where(col('DRVR_LIC_CLS_ID') != "UNLICENSED") \
                .select('VEH_MAKE_ID').groupBy('VEH_MAKE_ID').count().orderBy('count', ascending=False)

        w = Window().partitionBy(lit('a')).orderBy(lit('a'))
        df = df.withColumn("rank", row_number().over(w))
        df = df.where(col('rank') <= 5).drop('rank')

        return df





