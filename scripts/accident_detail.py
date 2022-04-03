from pyspark import SparkContext, Row
from pyspark.sql import SparkSession
import datetime
from pyspark.sql.functions import max, sum, first

from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()


class AccidentDetail:
    def __init__(self, output_path, spark_context=None):
        self.spark_context = spark_context
        self.output_path = output_path

    def count_accidents(self, person_gender, person_status, df_dict):
        df = df_dict["Primary_Person_use"]
        df = df.where((col("PRSN_INJRY_SEV_ID") == person_status) & (col("PRSN_GNDR_ID") == person_gender))
        df = df.select("CRASH_ID").distinct()
        return df.count()

    def count_accidents_state(self, person_gender, df_dict):
        df = df_dict["Primary_Person_use"]
        df = df.where(col("PRSN_GNDR_ID") == person_gender)
        df = df.groupBy("DRVR_LIC_STATE_ID").count()
        df = df.agg({'count': 'max', 'DRVR_LIC_STATE_ID': 'max'}).collect()[0]
        return df['max(DRVR_LIC_STATE_ID)']

    def count_damages(self, df_dict):
        df_damages = df_dict["Damages_use"]
        df_unit = df_dict["Units_use"]

        df = df_unit.join(df_damages, 'CRASH_ID', 'left').select(
            'VEH_DMAG_SCL_1_ID', 'VEH_DMAG_SCL_2_ID', 'FIN_RESP_TYPE_ID', 'CRASH_ID', 'DAMAGED_PROPERTY')
        df = df.where(col('DAMAGED_PROPERTY').contains("NO DAMAGE"))
        df.show()
        df = df.where((col('VEH_DMAG_SCL_1_ID') == "DAMAGED 4") | (col('VEH_DMAG_SCL_1_ID') == "DAMAGED 5") | (
                    col('VEH_DMAG_SCL_1_ID') == "DAMAGED 6") | (col('VEH_DMAG_SCL_1_ID') == "DAMAGED 7 HIGHEST"))
        df = df.where((col('VEH_DMAG_SCL_2_ID') == "DAMAGED 4") | (col('VEH_DMAG_SCL_2_ID') == "DAMAGED 5") | (
                col('VEH_DMAG_SCL_2_ID') == "DAMAGED 6") | (col('VEH_DMAG_SCL_2_ID') == "DAMAGED 7 HIGHEST"))
        df = df.where((col('FIN_RESP_TYPE_ID') != "NA"))
        df = df.select('CRASH_ID').distinct()
        return df.count()
