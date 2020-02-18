import pyspark
from pyspark.streaming import StreamingContext
from itertools import chain
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json, time
import pyspark.sql.functions as f

spark = pyspark.sql.SparkSession.builder \
    .appName("pysaprk_python") \
    .config('spark.cassandra.connection.host', '10.240.14.37') \
    .config('spark.cassandra.connection.port', '9042') \
    .getOrCreate()

schema = [
    'tweet_id',
    'tweet_content',
    'favorite_count',
    'quote_count',
    'retweeted_count',
    'timestamp'
]


# 트윗 랭킹 집계
def rank_tweet(df):
    print('rank 들어옴')
    # (favorite + quote + retweet) count 값 합산한 새로운 column 생성
    df = df.withColumn('total', df.favorite_count + df.quote_count + df.retweeted_count)
    # 같은 tweet_id 중 최신 것만 집계 (total 순으로)
    rank = df.groupBy(df.tweet_id).agg(
        f.first('tweet_content').alias('tweet_content'),
        f.first('total').alias('total'),
        f.max('timestamp').alias('timestamp')
    ).orderBy('total', ascending=False)
    rank.show()
    # 상위 랭킹 10개의 tweet 내용 리스트로 변환
    tweet_list = rank.select('tweet_content').rdd.flatMap(lambda x: x).take(10)


if __name__ == "__main__":
    lines = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="tweet_rank", keyspace="bts") \
        .load()
    current_time = int(time.time() * 1000000)  # 현재시간 마이크로 세컨즈 까지
    print(current_time)  # 현재시간 출력
    # 현재 시간 부터 30초 전까지 data 불러오기
    #         lines = lines.selectExpr(testschema)\
    #         .where((lines.timestamp >= current_time-30000000) & (lines.timestamp <= current_time) & (lines.retweeted == True)).limit(50).cache()
    lines = lines.select("*") \
        .limit(50).cache()
    # lines.show()
    rank_tweet(lines)
#     while True:
#         # 카산드라로부터 data 불러오기 (10초 마다)
#         lines = spark.read \
#             .format("org.apache.spark.sql.cassandra") \
#             .options(table="master_dataset", keyspace="bts") \
#             .load()
#         # 현재시간 마이크로 세컨즈 까지
#         current_time = int(time.time() * 1000000)
#         print(current_time)  # 현재시간 출력
#         # 현재 시간 부터 30초 전까지 data 불러오기
#         lines = lines.selectExpr(testschema) \
#             .where((lines.timestamp >= current_time - 30000000) & (lines.timestamp <= current_time)).limit(50).cache()
#         # lines.show()
#         rank_tweet(lines)
#         time.sleep(20)



