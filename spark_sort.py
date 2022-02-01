import sys
from pyspark.sql import SparkSession

# Took in input and output file command arguments
input_file = sys.argv[1]
output_file = sys.argv[2]
# Created spark session
spark = SparkSession.builder.appName("SortApp").getOrCreate()
# Read input file and convert to spark dataframe
inputData = spark.read.option("header",True).csv(input_file).cache()
# Sort input dataframe based on third and last columns
outputData = inputData.sort("cca2", "timestamp")
# Write out output dataframe to csv file destination
outputData.write.option("header", True).csv(output_file)
# End Spark Session
spark.stop()
