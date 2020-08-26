import json
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import *
from pyspark.sql import Window
from pyspark.sql.dataframe import DataFrame

spark = SparkSession\
    .builder\
    .master("local[*]")\
    .appName("spark_sql_test")\
    .getOrCreate()

# data = [("2017-02-05 12:01:00", "A", "[1,2,3]"),
#         ("2017-02-05 12:03:00", "B", "[4,5,3]"),
#         ("2017-02-05 12:06:00", "B", "[9,8,7]"),
#         ("2017-02-05 12:04:00", "A", "[3,2,3]"),
#         ("2017-02-05 12:22:00", "B", "[2,4,5]"),
#         ("2017-02-05 12:21:00", "A", "[9,6,3]"),
#         ("2017-02-05 12:22:00", "A", "[5,4,3]"),
#         ("2017-02-05 12:08:00", "B", "[3,2,5]"),
#         ("2017-02-05 12:11:00", "A", "[2,6,8]"),
#         ("2017-02-05 12:12:00", "B", "[8,7,3]")]
#
# df = spark.createDataFrame(data, ["access_time", "user_name", "items"])
#
# df = df.withColumn("access_time", f.unix_timestamp("access_time", "yyyy-MM-dd HH:mm:ss"))
#
# window = Window.partitionBy("user_name").orderBy("access_time")
#
# df2 = df.withColumn("before_time", f.lag("access_time", 1).over(window)).dropna(how="any")
#
# df2 = df2.withColumn("duration", f.col("access_time") - f.col("before_time"))
#
# df2.show()
#
# window2 = Window.partitionBy("user_name").orderBy(f.col("duration").desc())
#
# df2 = df2.withColumn("rank", f.rank().over(window2)).filter("rank <= 2")
#
# interesting_items = df2.select("user_name", "items")
#
# for r in interesting_items.collect():
#     print(type(r))
#     print(r.user_name, "==>", r.items)

schema = StructType([
    StructField("user_id", StringType()),
    StructField("items", ArrayType(StringType())),
    StructField("product_id", StringType()),
    StructField("checked", StringType()),
    StructField("quantity", StringType())
])

data = [(1, "a1,b1,c1,d1"),
        (2, "a2,b2,c2,d2"),
        (3, "a3,b3,c3,d3"),
        (4, "a4,b4,c4,d4"),
        (5, "a5,b5,c5,d5"),
        (6, "a6,b6,c6,d6"),
        (7, "a7,b7,c7,d7"),
        (8, "a8,b8,c8,d8"),
        (9, "a9,b9,c9,d9"),
        ]

data2 = [(1, "a1,b1,c1,d1"),
         (2, "a2,b2,c2,d2"),
         (3, "a3,b3,c3,d3"),
         (4, "a4,b4,c4,d4"),
         (5, "A5,B5,C5,D5"),
         (6, "A6,B6,C6,D6"),
         (7, "A7,B7,C7,D7"),
         (8, "A8,B8,C8,D8"),
         (9, "A9,B9,C9,D9"),
        ]
df = spark.createDataFrame(data, ["type", "value"])

df2 = spark.createDataFrame(data2, ["type", "value"])


try:
    d1 = df.select("type", f.explode(f.split("value", ",")).alias("u"))
    d2 = df2.select("type", f.explode(f.split("value", ",")).alias("u"))

    d1.union(d2).distinct().show(100)
    # d = df.select("type", f.explode(f.split("value", ",")).alias("u"))
    # d2 = d.filter("type <= 5")
    # d3 = d.unionAll(d2)
    # d3.show()
finally:
    spark.stop()
