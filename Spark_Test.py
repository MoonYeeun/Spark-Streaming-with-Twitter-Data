from pyspark import SparkContext
from pyspark.streaming import StreamingContext
# import pyspark
# spark = pyspark.sql.SparkSession.builder.appName("pysaprk_python").getOrCreate()
from pyspark.sql import *
from pyspark import StorageLevel

sc = SparkContext('local[2]',"test")
sqlContext = SQLContext(sc)
IP = "127.0.0.1"
Port = 5556
# 소켓을 통해 받아온 raw tweet 중 프로젝트에 필요한 부분들만 추출하는 작업

schema = [
    'created_at',
    'id',
    'text',
    'truncated',
    'in_reply_to_status_id',
    'in_reply_to_user_id',
    'in_reply_to_screen_name',
    'user',
    'is_quote_status',
    'entities',
    'retweeted',
    'lang',
    'timestamp_ms'
]
# 있을 수도 있고 없을 수도 있는 스키마
option_schema = [
    'is_quoted_status',
    'quoted_status',
    'extended_tweet',
    'quoted_status_permalink',
    'extended_tweet'
]
full_schema = [
    'created_at',
    'id',
    'text',
    'truncated',
    'in_reply_to_status_id',
    'in_reply_to_user_id',
    'in_reply_to_screen_name',
    'user',
    'is_quote_status',
    'entities',
    'retweeted',
    'lang',
    'timestamp_ms',
    'is_quoted_status',
    'quoted_status',
    'extended_tweet',
    'quoted_status_permalink',
    'extended_tweet'
]
# get DStream RDD
def getStreaming(data, schema=None):
  data.foreachRDD(process)
  return True

# Convenience function for turning JSON strings into DataFrames.
def process(rdd):
    try:
        jsonRDD = sqlContext.read.json(rdd).cache()
        jsonRDD.registerTempTable("tweets") #creates an in-memory table that is scoped to the cluster in which it was created.
        #jsonRDD.show()
        # print(jsonRDD.cache())
        # print(jsonRDD.count())
        #print(type(jsonRDD))
        print(jsonRDD.columns)
        col_lambda = lambda x : (x in full_schema)
        data = filter(col_lambda, jsonRDD.columns)
        print(list(data))
        # df = jsonRDD.select(data)
        # df.show()
    except:
        pass

if __name__ == "__main__":
    ssc = StreamingContext(sc, 10)
    #lines = ssc.socketTextStream(IP, Port)
    lines = ssc.socketTextStream(IP, Port, storageLevel=StorageLevel(True, True, False, False, 1))
    getStreaming(lines)
    ssc.start()
    ssc.awaitTermination()


