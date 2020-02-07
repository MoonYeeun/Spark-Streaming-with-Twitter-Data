from pyspark import SparkContext
from pyspark.streaming import StreamingContext
# import pyspark
# spark = pyspark.sql.SparkSession.builder.appName("pysaprk_python").getOrCreate()
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark import StorageLevel

sc = SparkContext('local[2]',"test")
sqlContext = SQLContext(sc)
IP = "127.0.0.1"
Port = 5556

# 소켓을 통해 받아온 raw tweet 중 트렌드에 필요한 부분들만 추출하는 작업

full_schema = [
    'created_at',
    'id',
    'text',
    'entities.hashtags.text as hashtag',
    'extended_tweet.full_text as extended_fullText',
    'extended_tweet.entities.hashtags.text as extended_hashtag',
    'retweeted_status.id as originTweet_id', # 리트윗 된 경우 원글
    'retweeted_status.text as originTweet_text',
    'retweeted_status.extended_tweet.full_text as originTweet_fullText',
    'retweeted_status.extended_tweet.entities.hashtags.text as originTweet_fullhashtag',
    'retweeted_status.quote_count as originTweet_quoteCount',
    'retweeted_status.retweet_count as originTweet_retweetCount',
    'retweeted_status.favorite_count as originTweet_favoriteCount',
    'is_quote_status',
    'quoted_status'
]

# get DStream RDD
def getStreaming(data, schema=None):
  data.foreachRDD(process)
  return True


# Convenience function for turning JSON strings into DataFrames.
def process(rdd):
    try:
        rawTweet = sqlContext.read.json(rdd).cache()
        rawTweet.registerTempTable("tweets") #creates an in-memory table that is scoped to the cluster in which it was created.
        rawTweet.selectExpr(full_schema).show()

    except:
        pass

if __name__ == "__main__":
    ssc = StreamingContext(sc, 10)
    #lines = ssc.socketTextStream(IP, Port)
    lines = ssc.socketTextStream(IP, Port, storageLevel=StorageLevel(True, True, False, False, 1))
    getStreaming(lines)
    ssc.start()
    ssc.awaitTermination()


