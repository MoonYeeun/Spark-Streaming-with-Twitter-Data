from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import *
from pyspark.sql import Row
import json
from pyspark.sql import SQLContext
from pyspark.sql.functions import from_json, Column
#create a local SparkSession, the starting point of all functionalities related to Spark.
spark = SparkSession.builder.appName("StructuredNetworkWordCount").getOrCreate()
sqlContext = SQLContext(spark.sparkContext)
# devColumns = [
#     StructField("created_at",StringType()),
#     StructField("id",LongType()),
# ]
mySchema = StructType([
      StructField("created_at", StringType(), True),
      StructField("id", LongType(), True)
  ])
#create a streaming DataFrame that represents text data received from a server listening on localhost:5555
lines = spark.readStream.format("socket").option("host", "localhost").option("port", 5556).load()
data1 = lines.select(
        # explode turns each item in an array into a separate row
        explode(
            split(lines.value, ',')
        )
    )
data2 = data1.select(
    explode(
        split(data1.col, ':')
    ).alias("key")
)
# def processing(data):
#

#     test = sqlContext.read.json(data.toJSON())
#     # myRow = Row(data1)
#     # schema = StructType([
#     #     StructField("id", LongType(), True),
#     #     StructField("created_at", StringType(), True),
#     # ])
#     # # Generate running word count
#     # person = sqlContext.createDataFrame(myRow, schema)
#     return test
# lines.select("value", from_json(Column("created_at"), mySchema))
# test = sqlContext.read.json(lines.toJSON())
# df = sqlContext.read.json("""{"userId": 1, "someString": "example1",
#         "Date": [20190101, 20190102, 20190103], "val": [1, 2, 9]}""")
# test = df.printSchema()
query = lines.writeStream\
        .format('console') \
        .start()

query.awaitTermination()
# sc = spark.sparkContext
# df = spark.read.json('/Users/yeeun/Documents/tweet.json', multiLine=True)
# df.printSchema()
# df.show()
#lines.writeStream.format("console").start().awaitTermination()

# devSchema = StructType(devColumns)
# devDF = spark.read.schema(devSchema).json(lines.map(lambda v: json.loads(v)))
