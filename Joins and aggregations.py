from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import sys

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path

os.environ['JAVA_HOME'] = r'C:\Users\kvrkr\.jdks\corretto-1.8.0_432'

conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()

spark.read.format("csv").load("data/test.txt").toDF("Success").show(20, False)


# data4 = [
#     (1, "raj"),
#     (2, "ravi"),
#     (3, "sai"),
#     (5, "rani")
# ]
#
# df1 = spark.createDataFrame(data4, ["id", "name"])
# df1.show()
#
# data3 = [
#     (1, "mouse"),
#     (3, "mobile"),
#     (7, "laptop")
# ]
#
# df2 = spark.createDataFrame(data3, ["id", "product"])
# df2.show()
#
# #INNER JOIN
# innerjoin = df1.join(df2, ["id"], "inner")
# innerjoin.show()
#
# #LEFT JOIN
# leftjoin = df1.join(df2, ["id"], "left").orderBy("id")
# leftjoin.show()
#
# #RIGHT JOIN
# rightjoin = df1.join(df2, ["id"], "right").orderBy("id")
# rightjoin.show()
#
# #FULL JOIN
# fulljoin = df1.join(df2, ["id"], "full").orderBy("id")
# fulljoin.show()
#
# #WHAT IF THE JOINING COLUMNS ARE DIFFERENTLY NAMED
# data4 = [
#     (1, "raj"),
#     (2, "ravi"),
#     (3, "sai"),
#     (5, "rani")
# ]
#
# df1 = spark.createDataFrame(data4, ["id", "name"])
# df1.show()
#
# data3 = [
#     (1, "mouse"),
#     (3, "mobile"),
#     (7, "laptop")
# ]
#
# df2 = spark.createDataFrame(data3, ["id1", "product"])
# df2.show()
#
# innerjoin = df1.join(df2, df1["id"] == df2["id1"], "inner")
# innerjoin.show()
# innerjoin = df1.join(df2, df1["id"] == df2["id1"], "inner").drop("id1")
# innerjoin.show()
#
# leftjoin = df1.join(df2, df1["id"] == df2["id1"], "left").orderBy("id").drop("id1")
# leftjoin.show()
#
# rightjoin = df1.join(df2, df1["id"] == df2["id1"], "right").orderBy("id1").drop("id")
# rightjoin.show()
#
# from pyspark.sql.functions import *
# fulljoin  = (
#             df1.join(df2, df1["id"] == df2["id1"], "full")
#             .withColumn("id", expr("CASE WHEN id IS NULL THEN id1 ELSE id END"))
#             .orderBy("id")
#             .drop("id1")
#             )
# fulljoin.show()
#
# #SCENARIO 1
# source_rdd = spark.sparkContext.parallelize([
#     (1, "A"),
#     (2, "B"),
#     (3, "C"),
#     (4, "D")
# ],1)
#
# target_rdd = spark.sparkContext.parallelize([
#     (1, "A"),
#     (2, "B"),
#     (4, "X"),
#     (5, "F")
# ],2)
#
#
# # Convert RDDs to DataFrames using toDF()
# df1 = source_rdd.toDF(["id", "name"])
# df2 = target_rdd.toDF(["id","name1"])
#
# # Show the DataFrames
# df1.show()
# df2.show()
#
#
# print("===== FULL JOIN=====")
# fulljoin = df1.join(df2, ["id"], "full")
# fulljoin.show()
#
# from pyspark.sql.functions import *
# print("=====NAME AND NAME 1 MATCH=====")
# procdf = fulljoin.withColumn("status", expr("CASE WHEN name == name1 THEN 'match' ELSE 'mismatch' END"))
# procdf.show()
#
# print("=====FILTER MISMATCH=====")
# fildf = procdf.filter("status = 'mismatch'")
# fildf.show()
#
# print("=====NULL CHECKS=====")
# procdf1 = (
#             fildf.withColumn("status",expr("""
#                                 CASE
#                                 WHEN name1 IS NULL THEN 'New in Source'
#                                 WHEN name IS NULL THEN 'New in Target'
#                                 ELSE status
#                                 END
#             """))
# )
# procdf1.show()
#
#
# print("=====FINAL PROC=====")
# finaldf = procdf1.drop("name", "name1").withColumnRenamed("status", "comment")
# finaldf.show()
#
#
# data = [(1,"Veg Biryani"),(2,"Veg Fried Rice"),(3,"Kaju Fried Rice"),(4,"Chicken Biryani"),(5,"Chicken Dum Biryani"),(6,"Prawns Biryani"),(7,"Fish Birayani")]
#
# df1 = spark.createDataFrame(data,["food_id","food_item"])
# df1.show()
#
# ratings = [(1,5),(2,3),(3,4),(4,4),(5,5),(6,4),(7,4)]
#
# df2 = spark.createDataFrame(ratings,["food_id","rating"])
# df2.show()
#
# from pyspark.sql.functions import *
# leftjoin = df1.join(df2, "food_id", "left").orderBy("food_id").withColumn("stats(Out of 5)", expr("repeat('*',rating)"))
# leftjoin.show()
#
#
# #ANTI JOIN
# data4 = [
#     (1, "raj"),
#     (2, "ravi"),
#     (3, "sai"),
#     (5, "rani")
# ]
#
# df1 = spark.createDataFrame(data4, ["id", "name"])
# df1.show()
#
# data3 = [
#     (1, "mouse"),
#     (3, "mobile"),
#     (7, "laptop")
# ]
#
# df2 = spark.createDataFrame(data3, ["id", "product"])
# df2.show()
#
# #WRONG METHOD WHICH WILL LEAD TO SKEWNESS
# listval = df2.select("id").rdd.flatMap(lambda x:x).collect()
# print(listval)
#
# from pyspark.sql import functions as F
# finaldf = df1.filter(~F.col('id').isin(listval))
# finaldf.show()
#
# #CORRECT METHOD
# antijoin = df1.join(df2, "id", "left_anti")
# antijoin.show()
#
#
# #CROSS JOIN
# crossjoin = df1.crossJoin(df2.withColumnRenamed("id","id1"))
# crossjoin.show()


#
# data = [("A", "AA"), ("B", "BB"), ("C", "CC"), ("AA", "AAA"), ("BB", "BBB"), ("CC", "CCC")]
#
# df = spark.createDataFrame(data, ["child", "parent"])
# df.show()
#
# df1 = df
# df2 = df.withColumnRenamed("child","child1").withColumnRenamed("parent","parent1")
#
# df1.show()
# df2.show()
#
# joindf = df1.join(df2, df1["child"] ==  df2["parent1"])
# joindf.show()
#
# finaldf = (
#             joindf
#             .drop("parent1")
#             .withColumnRenamed("child","parent1")
#             .withColumnRenamed("parent","grand parent")
#             .withColumnRenamed("child1","child")
#             .withColumnRenamed("parent1","parent")
#             .select("child","parent","grand parent")
# )
# finaldf.show()


# #AGGREGATION
# data = [("sai", 40), ("zeyo", 30), ("sai", 50), ("zeyo", 40), ("sai", 10)]
# df = spark.createDataFrame(data, ["name", "amount"])
# df.show()
#
#
# from pyspark.sql.functions import *
# aggdf = (
#         df
#         .groupBy("name")
#         .agg(sum("amount").alias("total"), count("name").alias("cnt"))
# )
# aggdf.show()

#
# data1 = [("sai","chennai", 40), ("sai","hydb", 50), ("sai","chennai", 10), ("sai","hydb", 60)]
# df1 = spark.createDataFrame(data1, ["name", "location", "amount"])
# df1.show()
#
# from pyspark.sql.functions import *
# aggdf1 = (
#             df1
#             .groupBy("name","location")
#             .agg(sum("amount").alias("total"))
# )
# aggdf1.show()


from pyspark.sql.functions import *
#TASK 1
data1 = [
    (1, "A", "A", 1000000),
    (2, "B", "A", 2500000),
    (3, "C", "G", 500000),
    (4, "D", "G", 800000),
    (5, "E", "W", 9000000),
    (6, "F", "W", 2000000),
]

df1 = spark.createDataFrame(data1, ["emp_id","name","dept_id","salary"])
df1.show()

data2 = [("A", "AZURE"), ("G", "GCP"), ("W", "AWS")]
df2 = spark.createDataFrame(data2, ["dept_id1", "dept_name"])
df2.show()

joindf = df1.join(df2, df1["dept_id"] == df2["dept_id1"], "left" )
joindf.show()

seldf = joindf.select("emp_id","name","dept_name","salary").orderBy("dept_name","salary")
seldf.show()

finaldf = (
            seldf
            .groupBy("dept_name")
            .agg(min("salary").alias("salary"))
)
finaldf.show()
