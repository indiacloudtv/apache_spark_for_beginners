from pyspark.sql import SparkSession

import sys
import time

if __name__ == "__main__":
    print("Apache Spark Application using Scala - YARN Log File - Starts Here ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    if len(sys.argv) == 1:
        program_name = sys.argv[0]
        print("program_name: " + program_name)
    else:
        print("Failed, no input arguments.")
        exit(1)

    spark = SparkSession \
        .builder \
        .appName("Apache Spark Application using Scala - YARN Log File - Demo") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    yarn_log_file_path = "file:///D://record_tech_videos//spark_for_beginners//sample_yarn_logs//application_1551777740169_0255.txt"
    lines_rdd = spark.sparkContext.textFile(yarn_log_file_path)

    error_lines_rdd = lines_rdd.filter(lambda line: "ERROR" in line)

    error_lines_rdd_count = error_lines_rdd.count()
    print("\n")
    print("error_lines_rdd_count: " + str(error_lines_rdd_count))
    print("\n")
    print(error_lines_rdd.toDebugString())
    print("\n")
    output = error_lines_rdd.collect()
    print(output)
    print("\n")
    spark.stop()

    print("Apache Spark Application using Scala - YARN Log File - Ends Here.")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))