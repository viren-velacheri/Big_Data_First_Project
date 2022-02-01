import sys
from pyspark.sql import SparkSession

input_file = sys.argv[1]
output_file = sys.argv[2]
spark = SparkSession.builder.appName("SortApp").getOrCreate()
inputData = spark.read.csv(input_file).cache()
outputData = inputData.sort("cca2", "timestamp").collect()
outputData.write.csv(output_file)
spark.stop()
