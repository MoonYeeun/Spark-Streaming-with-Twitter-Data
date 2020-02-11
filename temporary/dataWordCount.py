from pyspark.streaming import StreamingContext
import pyspark
spark = pyspark.sql.SparkSession.builder.appName("pysaprk_python").getOrCreate()
from pyspark import StorageLevel
from konlpy.tag import Okt
from nltk.corpus import stopwords
import re
# $example on$
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, RegexTokenizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import NGram
from sklearn.feature_extraction.text import TfidfVectorizer
# $example off$
from pyspark.sql import SparkSession

# sc = SparkContext('local[2]',"test")
# sqlContext = SQLContext(sc)
IP = "127.0.0.1"
Port = 5556

# tweet text word count

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
    '방탄소년단',
    'TWICE','twice','Twice','https','http'
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
    data.foreachRDD(process)
    return True

# text 에서 어절 추출
def get_word(text):
    # 문장을 단어 단위로 쪼갬
    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    wordsData = tokenizer.transform(text)
    #wordsData.show()

    # # 불용어 제거
    # remover = StopWordsRemover() \
    #         .setStopWords(mystopwords) \
    #         .setCaseSensitive(False) \
    #         .setInputCol("words") \
    #         .setOutputCol("filtered")
    # remover.transform(wordsData).show()


    # tf 벡터화 과정- HashingTF to hash the sentence into a feature vector.
    hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=50)
    featurizedData = hashingTF.transform(wordsData)
    featurizedData.show()

    # idf 벡터화 과정 - IDF to rescale the feature vectors
    # idf = IDF(inputCol="rawFeatures", outputCol="features")
    # idfModel = idf.fit(featurizedData)  # fit 명령어를 통해서 text 변수에 저장된 데이터를 학습

    # tf-idf 벡터화 최종 결과
    # rescaledData = idfModel.transform(featurizedData)
    # rescaledData.show()
    result = featurizedData.select('words','rawFeatures').rdd.map(lambda x: x)
    for i in result.collect():
        print(i)
    # $example off$
    return True

def get_Ngram(text):
    # 문장을 단어 단위로 쪼갬
    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    wordsData = tokenizer.transform(text)
    # 불용어 제거
    # remover = StopWordsRemover() \
    #         .setStopWords(mystopwords) \
    #         .setCaseSensitive(False) \
    #         .setInputCol("words") \
    #         .setOutputCol("filtered")
    # remover.transform(wordsData).show(truncate = 15)

    # N-gram 이용하여 단어 조합 만들기
    ngram = NGram(n=3, inputCol="words", outputCol="ngrams")
    ngramDataFrame = ngram.transform(wordsData)
    result = ngramDataFrame.select("ngrams").show(truncate=False)
    ngr = ngram.rdd.flatmap(lambda x : x).collect()
    for i in ngr:
        print(i)
    return result

def get_TFIDF(text):
    #tfidfv = TfidfVectorizer().fit(text)
    result = []
    for i in text:
        list = []
        list.append(i)
        # vectorizer = TfidfVectorizer(ngram_range=(1, 3))
        # tfidf = vectorizer.fit(list)
        tfidf = TfidfVectorizer(stop_words=mystopwords).fit(list)
        feature_names = tfidf.get_feature_names()
        print(feature_names)
        for j in feature_names:
            result.append(j)


    # result = sorted(vectorizer.vocabulary_.items())

    print(result)
    # for i in feature_names:
    #     print(i)
    #print(tfidfv.vocabulary_)
    return result

# Convenience function for turning JSON strings into DataFrames.
def process(rdd):
    try:
        rawTweet = spark.read.json(rdd)
        rawTweet.registerTempTable("tweets") #creates an in-memory table that is scoped to the cluster in which it was created.
        result = rawTweet.selectExpr(schema)
        #get_word(result)
        # ngram = get_Ngram(result)
        # nresult = ngram.rdd.map(lambda x : x[0]).collect()
        # print(nresult)
        # #word_count(nresult.collect())

        #result = rawTweet.select('text').rdd.flatMap(lambda x : x)
        # print(result.collect())
        #tfidf = get_TFIDF(result.collect())
        # 현재 타임에 들어온 tweet text 중 의미있는 어절만 추출
        #result = get_word(result.collect())
        #print(result)

        #word count 작업을 위해 결과 rdd로 만들어줌
        # rdd = spark.sparkContext.parallelize(tfidf)
        # word_count(rdd)

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
