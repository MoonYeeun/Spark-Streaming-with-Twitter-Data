import pyspark
sc = pyspark.SparkConf()\
    .setMaster("local[*]")\
    .set("spark.driver.memory","8g")\
    .set("spark.executor.memory","8g")\
    .set("spark.debug.maxToStringFields", 10000)\
    .set('spark.sql.debug.maxToStringFields', 2000)\
    .set("spark.jars","/Users/yeeun/Apache/spark-2.4.4-bin-hadoop2.7/jars/spark-redis-2.4.0-jar-with-dependencies.jar")

sparkContext = pyspark.SparkContext(conf=sc)

spark = pyspark.sql.SparkSession(sparkContext).builder\
    .appName("pysaprk_python")\
    .config("spark.redis.host", "localhost")\
    .config("spark.redis.port", "6379")\
    .getOrCreate()

from pyspark.streaming import StreamingContext
from pyspark import StorageLevel
from itertools import chain
import redis, json, time
myRedis = redis.Redis(host='127.0.0.1', port=6379, db=0)

IP = "127.0.0.1"
Port = 5559

# tweet hashtag Trend analysis

schema = [
    'text',
    'is_quote_status',
    'entities.hashtags.text as hashtag'
]

# 분석 대상에서 제외할 단어 명시
mystopwords = [
    'RT',
    'BTS',
    'bts',
    'Bts',
    '방탄소년단'
]
similarwords = [
    ['정국', 'JUNGKOOK'],
    ['지민', 'JIMIN'],
    ['제이홉', 'JHOPE'],
    ['슈가', 'SUGA'],
    ['태형', '김태형', 'V', 'TAEHYUNG', '뷔'],
    ['남준', '김남준', '랩몬', 'RM'],
    ['석진', '진', '김석진', 'JIN']
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
    data.foreachRDD(process) # 각 rdd 별로 처리
    return True

# hashtag 전처리
def hashtag_processing(text):
    total = list(chain.from_iterable(text)) # 리스트 안에 리스트 하나의 리스트로 합치기
    result = []
    # 불용어 제거
    for i in total:
        if i not in mystopwords:
            result.append(i)
    # 유사어 제거
    words = '-'.join(result)
    words = words.upper()

    for i in similarwords[0]:
        if i in words:
            words = words.replace(i, 'Jungkook')
    for i in similarwords[1]:
        if i in words:
            words = words.replace(i, 'Jimin')
    for i in similarwords[2]:
        if i in words:
            words = words.replace(i, 'JHope')
    for i in similarwords[3]:
        if i in words:
            words = words.replace(i, 'Suga')
    for i in similarwords[4]:
        if i in words:
            words = words.replace(i, 'Taehyung')
    for i in similarwords[5]:
        if i in words:
            words = words.replace(i, 'RM')
    for i in similarwords[6]:
        if i in words:
            words = words.replace(i, 'JIN')
    result = words.split('-')

    print('응 들어옴')
    print(result)
    return result

# Convenience function for turning JSON strings into DataFrames.
def process(rdd):
    try:
        rawTweet = spark.read.json(rdd)
        rawTweet.registerTempTable("tweets") #creates an in-memory table that is scoped to the cluster in which it was created.
        tag = rawTweet.selectExpr(schema)
        hashtag = tag.select('hashtag').rdd.flatMap(lambda x : x)
        print(hashtag.collect())
        # 현재 타임에 들어온 hashtag 전처리
        result = hashtag_processing(hashtag.collect())

        #word count 작업을 위해 결과 rdd로 만들어줌
        rdd = spark.sparkContext.parallelize(result)
        word_count(rdd)

    except:
        pass

# 추출된 단어 word count
def word_count(list):
    print('word count 들어옴')
    pairs = list.map(lambda word: (word, 1))
    # 상위 10개만 가져오기 + 등장빈도 2번 이상
    #wordCounts = pairs.reduceByKey(lambda x, y: x + y).takeOrdered(10, lambda args:-args[1])
    wordCounts = pairs.reduceByKey(lambda x, y: x + y).filter(lambda args : args[1] > 2)
    #print(wordCounts)
    ranking = wordCounts.takeOrdered(10, lambda args:-args[1])
    print(ranking)
    # key : 현재 시간 , value : 순위 결과 json 으로 redis 저장
    current_time = time.strftime("%d/%m/%Y")
    rank_to_json = json.dumps(ranking)
    myRedis.set(current_time, rank_to_json, ex=60*5)


if __name__ == "__main__":
    spark.conf
    ssc = StreamingContext(sparkContext, 20)
    #ssc.checkpoint("checkpoint")
    lines = ssc.socketTextStream(IP, Port, storageLevel=StorageLevel(True, True, False, False, 1))
    getStreaming(lines)
    ssc.start()
    ssc.awaitTermination()
