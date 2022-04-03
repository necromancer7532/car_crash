import sys
import json
import os
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
    logger.warning("No configs passed using default config")
code_path = os.path.realpath(__file__)
file_path = '\\'.join(code_path.split('\\')[:-2]) + '\\input_files'
config_file = sys.argv[1]
f = open(config_file)
config_data = json.load(f)
config_data = config_data["questions"]
################################################# Analysis 1 #################################################
config_for_question = config_data[0]
file_read_obj = FileRead(spark)

df_file = file_read_obj.read_data_from_file(file_path, config_for_question["file_used"], "csv")

accident_count_obj = AccidentCount(spark)

count_accident = accident_count_obj.count_accidents("MALE", "KILLED", df_file)

logger.info("Analysis 1 : Number of accidents where a male died : {}".format(count_accident))

