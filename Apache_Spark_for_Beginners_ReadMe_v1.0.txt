Launch PySpark CLI:
-------------------

pyspark --master spark://instance-1-master-node.us-east1-b.c.nifty-vault-240207.internal:7077


Create RDD:

tech_names_list = ["spark1", "spark2", "spark3", "hadoop1", "hadoop2", "spark4"]

tech_names_list

names_rdd = spark.sparkContext.parallelize(tech_names_list, 2)

names_rdd

spark_names_rdd = names_rdd.filter(lambda item: "spark" in item)

spark_names_rdd

spark_names_upper_rdd = spark_names_rdd.map(lambda item: item.upper())

spark_names_upper_rdd

spark_names_upper_rdd.count()

spark_names_upper_rdd.collect()


spark_names_upper_rdd.take(2)


