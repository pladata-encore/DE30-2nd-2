from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, BooleanType,IntegerType,FloatType

def makeSchema():
    schema = StructType([
        StructField("spotify_id", StringType()),
        StructField("name", StringType()),
        StructField("artists", StringType()),
        StructField("daily_rank", StringType()),
        StructField("country", StringType()),
        StructField("snapshot_date", StringType()),
        StructField("popularity", StringType()),
        StructField("is_explicit", StringType()),
        StructField("duration_ms", StringType()),
        StructField("album_name", StringType()),
        StructField("album_release_date", StringType()),
        StructField("danceability", StringType()),
        StructField("energy", StringType()),
        StructField("key", StringType()),
        StructField("loudness", StringType()),
        StructField("mode", StringType()),
        StructField("speechiness", StringType()),
        StructField("acousticness", StringType()),
        StructField("instrumentalness", StringType()),
        StructField("liveness", StringType()),
        StructField("valence", StringType()),
        StructField("tempo", StringType()),
        StructField("time_signature", StringType())
    ])
    return schema

def spotify_KafkaToParquet(kafka_topic, output_path):
    spark = SparkSession.builder \
        .appName("StructuredStreamingFromKafka") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.debug.maxToStringFields", "100") \
        .getOrCreate()
    
    # Kafka에서 데이터 읽기
    kafka_df = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("startingOffsets", "earliest") \
        .option("subscribe", kafka_topic) \
        .load()
    
    # JSON 데이터의 스키마 정의
    schema = makeSchema()
    
    # JSON 데이터 파싱 및 스키마 적용
    parsed_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data"))
    
    # 필요한 필드만 선택하여 결과를 정리
    flattened_df = parsed_df.select(
        col("data.spotify_id").alias("spotify_id"),
        col("data.name").alias("name"),
        col("data.artists").alias("artists"),
        col("data.daily_rank").alias("daily_rank"),
        col("data.country").alias("country"),
        col("data.snapshot_date").alias("snapshot_date"),
        col("data.popularity").alias("popularity"),
        col("data.is_explicit").alias("is_explicit"),
        col("data.duration_ms").cast(IntegerType()).alias("duration_ms"),
        col("data.album_name").alias("album_name"),
        col("data.album_release_date").alias("album_release_date"),
        col("data.danceability").cast(FloatType()).alias("danceability"),
        col("data.energy").cast(FloatType()).alias("energy"),
        col("data.key").cast(IntegerType()).alias("key"),
        col("data.loudness").cast(FloatType()).alias("loudness"),
        col("data.mode").cast(IntegerType()).alias("mode"),
        col("data.speechiness").cast(FloatType()).alias("speechiness"),
        col("data.acousticness").cast(FloatType()).alias("acousticness"),
        col("data.instrumentalness").cast(FloatType()).alias("instrumentalness"),
        col("data.liveness").cast(FloatType()).alias("liveness"),
        col("data.valence").cast(FloatType()).alias("valence"),
        col("data.tempo").cast(FloatType()).alias("tempo"),
        col("data.time_signature").cast(IntegerType()).alias("time_signature")
    )
    
    # Parquet 파일로 저장
    flattened_df.write \
        .mode("append") \
        .parquet(output_path)
