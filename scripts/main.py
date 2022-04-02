from pyspark import SparkContext
from pyspark.sql import SparkSession
import datetime
from file_read_util import FileRead
from accident_count import AccidentCount

spark = SparkSession.builder.master("local").appName("car_crash").getOrCreate()

log4jLogger = spark._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger(__name__)
logger.setLevel(log4jLogger.Level.INFO)

file_read_obj = FileRead(spark)
df_file = file_read_obj.read_data_from_file("./input_files/Primary_Person_use.csv","csv")

accident_count_obj = AccidentCount(spark)

count_accident = accident_count_obj.count_accidents("MALE", "KILLED", df_file)

print(count_accident)


