from pyspark import SparkContext
from pyspark.streaming import StreamingContext
# import pyspark
# spark = pyspark.sql.SparkSession.builder.appName("pysaprk_python").getOrCreate()
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import StorageLevel
import json

sc = SparkContext('local[2]',"test")
sqlContext = SQLContext(sc)
IP = "127.0.0.1"
Port = 5556

# 소켓을 통해 받아온 raw tweet 중 프로젝트에 필요한 부분들만 추출하는 작업

full_schema = [
    'created_at',
    'id',
    'text',
    'truncated',
    'user',
    'in_reply_to_status_id',
    'in_reply_to_user_id',
    'in_reply_to_screen_name',
    'quoted_status', # data 처리
    'quoted_status_permalink',
    'is_quote_status',
    'entities',
    'retweeted',
    'lang',
    'timestamp_ms'
]
user_schema = [
    'user.id',
    'user.name',
    'user.screen_name',
    'user.location',
    'user.url',
    'user.description',
    'user.followers_count',
    'user.friends_count',
    'user.listed_count',
    'user.favourites_count',
    'user.statuses_count',
    'user.created_at',
    'user.profile_image_url',
    'user.profile_banner_url'
]

# get DStream RDD
def getStreaming(data, schema=None):
  data.foreachRDD(process)
  #data.foreachRDD(user_rowProcess)
  return True

# user schema 중 원하는 부분만 추출하기 위한 작업
def user_rowProcess(rdd):
    raw = sqlContext.read.json(rdd).cache()
    #raw.select('user').show()
    user = raw.select(user_schema).toJSON()

    for i in user.collect():
        print(i)
    #user = raw.select(user_schema)
    #user.show()

# Convenience function for turning JSON strings into DataFrames.
def process(rdd):
    try:
        #user_result = user_rowProcess(rdd)
        rawTweet = sqlContext.read.json(rdd).cache()
        rawTweet.registerTempTable("tweets") #creates an in-memory table that is scoped to the cluster in which it was created.
        #user = rawTweet.select(user_schema).rdd.collect()
        # user = rawTweet.select(user_schema).rdd.collect().map(lambda row: row[0]).collect()
        # print(user)
        # userlist = [row for row in rawTweet.select(user_schema).collect()]
        # print(userlist)
        # for x in userlist:
        #     print(x)

        # 필요한 col 추출하여 새로운 데이터프레임 생성
        col_lambda = lambda x : (x in full_schema)
        col = filter(col_lambda, rawTweet.columns)
        df = rawTweet.select(full_schema)
        df.show()

    except:
        pass

if __name__ == "__main__":
    ssc = StreamingContext(sc, 10)
    #lines = ssc.socketTextStream(IP, Port)
    lines = ssc.socketTextStream(IP, Port, storageLevel=StorageLevel(True, True, False, False, 1))
    getStreaming(lines)
    ssc.start()
    ssc.awaitTermination()


