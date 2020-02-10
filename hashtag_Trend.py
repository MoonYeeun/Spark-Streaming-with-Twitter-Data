from pyspark.streaming import StreamingContext
import pyspark
spark = pyspark.sql.SparkSession.builder.appName("pysaprk_python").getOrCreate()
from pyspark import StorageLevel
from itertools import chain
from konlpy.tag import Okt

# sc = SparkContext('local[2]',"test")
# sqlContext = SQLContext(sc)
IP = "127.0.0.1"
Port = 5556

# tweet hashtag Trend analysis

schema = [
    'text',
    'is_quote_status',
    'entities.hashtags.text as hashtag'
]
quote_exist_schema = [
    'text',
    'quoted_status.text as quoted_text'
]
# 분석 대상에서 제외할 단어 명시
mystopwords = [
    'RT',
    'BTS',
    'bts',
    'Bts',
    '방탄소년단'
]

option_schema = [
    # extend tweet 존재 할 경우
    'extended_tweet.full_text as extended_text',
    'extended_tweet.entities.hashtags.text as extended_hashtag',
    # quoted tweet 존재 할 경우
    'quoted_status.text as quoted_text'
]

# get DStream RDD
def getStreaming(data, schema=None):
    data.pprint() # 실시간으로 들어오는 tweet 출력
    data.foreachRDD(process) # 각각 rdd 별로 수
    return True

# hashtag 전처리
def hashtag_processing(text):
    total = list(chain.from_iterable(text)) # 리스트 안에 리스트 하나의 리스트로 합치기
    result = []
    # 불용어 제거
    for i in total:
        if i not in mystopwords:
            result.append(i)

    print('응 들어옴')
    print(result)
    return result

# Convenience function for turning JSON strings into DataFrames.
def process(rdd):
    try:
        rawTweet = spark.read.json(rdd)
        rawTweet.registerTempTable("tweets") #creates an in-memory table that is scoped to the cluster in which it was created.
        tag = rawTweet.selectExpr(schema)
        #tag.show()
        hashtag = tag.select('hashtag').rdd.flatMap(lambda x : x)
        print(hashtag.collect())
        # 현재 타임에 들어온 tweet text 중 의미있는 어절만 추출
        result = hashtag_processing(hashtag.collect())
        #print(result)

        #word count 작업을 위해 결과 rdd로 만들어줌
        rdd = spark.sparkContext.parallelize(result)
        word_count(rdd)

    except:
        pass

# 추출된 단어 word count
def word_count(list):
    print('word count 들어옴')
    pairs = list.map(lambda word: (word, 1))
    # 상위 10개만 가져오기
    wordCounts = pairs.reduceByKey(lambda x, y: x + y).takeOrdered(10, lambda args:-args[1])
    print(wordCounts)


if __name__ == "__main__":
    spark.conf.set("spark.debug.maxToStringFields", 10000)
    spark.conf.set('spark.sql.debug.maxToStringFields', 2000)
    ssc = StreamingContext(spark.sparkContext, 10)
    #lines = ssc.socketTextStream(IP, Port)
    #ssc.checkpoint("checkpoint")
    lines = ssc.socketTextStream(IP, Port, storageLevel=StorageLevel(True, True, False, False, 1))
    getStreaming(lines)
    ssc.start()
    ssc.awaitTermination()
