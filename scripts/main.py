import sys
import json
import os
from pyspark import SparkContext
from pyspark.sql import SparkSession
import datetime
from file_read_util import FileRead
from accident_detail import AccidentDetail
from vehicle_booked import VehicleBooked
from person_involved import PersonInvolved

spark = SparkSession.builder.master("local").appName("car_crash").getOrCreate()

log4jLogger = spark._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger(__name__)
logger.setLevel(log4jLogger.Level.INFO)


code_path = os.path.realpath(__file__)
file_path = '\\'.join(code_path.split('\\')[:-2]) + '\\input_files'
output_path = '\\'.join(code_path.split('\\')[:-2]) + '\\output_files'
if len(sys.argv) < 3:
    logger.info("No configs passed using default config")
    config_file = '\\'.join(code_path.split('\\')[:-2]) + '\\configs\\config.json'
    question_no = sys.argv[1]
else:
    config_file = sys.argv[1]
    question_no = sys.argv[2]
f = open(config_file)
config_data = json.load(f)
config_data = config_data["questions"]
################################################# Analysis 1 #################################################
if question_no == '1':
    config_for_question = config_data[0]
    file_read_obj = FileRead(spark)

    df_dict = file_read_obj.read_data_from_file(file_path, config_for_question["file_used"], "csv")

    accident_count_obj = AccidentDetail(output_path, spark)

    count_accident = accident_count_obj.count_accidents("MALE", "KILLED", df_dict)

    logger.info("Analysis 1 : Number of accidents where a male died : {}".format(count_accident))
    spark.stop()
################################################# Analysis 2 #################################################
elif question_no == '2':
    config_for_question = config_data[1]
    file_read_obj = FileRead(spark)

    df_dict = file_read_obj.read_data_from_file(file_path, config_for_question["file_used"], "csv")

    vehicle_booked_obj = VehicleBooked(spark)

    count_vehicles = vehicle_booked_obj.booked_vehicle("MOTORCYCLE", df_dict)
    logger.info("Analysis 2 : Number of 2 wheelers booked for crashes : {}".format(count_vehicles))
    spark.stop()
################################################# Analysis 3 #################################################
elif question_no == '3':
    config_for_question = config_data[2]
    file_read_obj = FileRead(spark)

    df_dict = file_read_obj.read_data_from_file(file_path, config_for_question["file_used"], "csv")

    accident_count_obj = AccidentDetail(output_path, spark)

    count_accident = accident_count_obj.count_accidents_state("FEMALE", df_dict)

    logger.info("Analysis 3 : State with max number of accident involving females : {}".format(count_accident))
    spark.stop()
################################################# Analysis 4 #################################################
elif question_no == '4':
    config_for_question = config_data[3]
    file_read_obj = FileRead(spark)

    df_dict = file_read_obj.read_data_from_file(file_path, config_for_question["file_used"], "csv")

    vehicle_booked_obj = VehicleBooked(spark)

    df_result = vehicle_booked_obj.vehicle_model_count(df_dict)

    logger.info("Analysis 4 : Top 5th to 15th VEH_MAKE_IDs that contribute to the largest number of injuries "
                "including death :")
    df_result.show()
    spark.stop()
################################################# Analysis 5 #################################################
elif question_no == '5':
    config_for_question = config_data[4]
    file_read_obj = FileRead(spark)

    df_dict = file_read_obj.read_data_from_file(file_path, config_for_question["file_used"], "csv")

    person_involved_obj = PersonInvolved(spark)

    df_result = person_involved_obj.ethnicity_count_bodystyle(df_dict)
    logger.info("the top ethnic user group of each unique body style : ")
    df_result.show()
    spark.stop()
################################################# Analysis 6 #################################################
elif question_no == '6':
    config_for_question = config_data[5]
    file_read_obj = FileRead(spark)

    df_dict = file_read_obj.read_data_from_file(file_path, config_for_question["file_used"], "csv")
    person_involved_obj = PersonInvolved(spark)

    df_result = person_involved_obj.zip_code_crashes(df_dict)
    logger.info("Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash")
    df_result.show()
    spark.stop()
################################################# Analysis 7 #################################################
elif question_no == '7':
    config_for_question = config_data[6]
    file_read_obj = FileRead(spark)

    df_dict = file_read_obj.read_data_from_file(file_path, config_for_question["file_used"], "csv")
    accident_count_obj = AccidentDetail(output_path, spark)
    count_crashes = accident_count_obj.count_damages(df_dict)
    logger.info("Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level is above 4 and car avails Insurance : {}".format(count_crashes))
    spark.stop()
################################################# Analysis 8 #################################################
elif question_no == '8':
    config_for_question = config_data[7]
    file_read_obj = FileRead(spark)

    df_dict = file_read_obj.read_data_from_file(file_path, config_for_question["file_used"], "csv")

    vehicle_booked_obj = VehicleBooked(spark)
    df_result = vehicle_booked_obj.vehicle_make(df_dict)
    logger.info("the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed "
                "Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest "
                "number of offences")
    df_result.show()
    spark.stop()
###################################################################################################################

else:
    print("invalid Question number provided please provide the right question number")
    spark.stop()
