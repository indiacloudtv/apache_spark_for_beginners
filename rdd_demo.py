from pyspark.sql import SparkSession

import sys
import time

if __name__ == "__main__":
    print("First Apache Spark Application using Scala Starts Here ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    if len(sys.argv) == 1:
        program_name = sys.argv[0]
        print("program_name: " + program_name)
    else:
        print("Failed, no input arguments.")
        exit(1)

    spark = SparkSession \
        .builder \
        .appName("First Apache Spark Application using Scala - Demo") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    tech_names_list = ["spark1", "spark2", "spark3", "hadoop1", "hadoop2", "spark4"]

    names_rdd = spark.sparkContext.parallelize(tech_names_list, 2)

    names_upper_case_rdd = names_rdd.map(lambda item: item.upper())

    names_upper_case_spark_rdd = names_upper_case_rdd.filter(lambda item: "SPARK" in item)
    print("\n")
    print(names_upper_case_spark_rdd.toDebugString())
    print("\n")
    output = names_upper_case_spark_rdd.collect()
    print(output)
    print("\n")
    spark.stop()

    print("First Apache Spark Application using Scala Ends Here.")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))