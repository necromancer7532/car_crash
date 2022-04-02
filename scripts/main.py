import sys

from pyspark import SparkContext
from pyspark.sql import SparkSession
import datetime
from file_read_util import FileRead
from accident_count import AccidentCount

spark = SparkSession.builder.master("local").appName("car_crash").getOrCreate()

log4jLogger = spark._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger(__name__)
logger.setLevel(log4jLogger.Level.INFO)

if len(sys.argv) < 2:
    logger.error("PLEASE PASS THE FILE LOCATION AS ARGUMENT")
    exit(0)
file_path = sys.argv[1]
################################################# Analysis 1 #################################################
file_read_obj = FileRead(spark)
df_file = file_read_obj.read_data_from_file(file_path+"/Primary_Person_use.csv", "csv")

accident_count_obj = AccidentCount(spark)

count_accident = accident_count_obj.count_accidents("MALE", "KILLED", df_file)

logger.info("Analysis 1 : Number of accidents where a male died : {}".format(count_accident))

