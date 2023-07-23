import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, desc, row_number
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import IntegerType, DateType
from pyspark.sql.window import Window

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.selectExpr('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_data + 'songs_table')

    # extract columns to create artists table
    artists_table = df.selectExpr('artist_id', 'artist_name as name', 'artist_location as location',
                                  'artist_latitude as latitude', 'artist_longitude as longitude').dropDuplicates()

    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + 'artists_table')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')

    # create window function for unique users
    users_window = Window.partitionBy('userId').orderBy(desc('ts'))

    # add window function for checking user ocurrence
    df = df.withColumn("row_number", row_number().over(users_window))

    # extract columns for users table
    users_table = df.where(df.row_number == 1).selectExpr('userId as user_id', 'firstName as first_name',
                                                          'lastName as last_name', 'gender', 'level')

    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + 'users_table')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts_ms: int(int(ts_ms) / 1000), IntegerType())
    df = df.withColumn("timestamp", get_timestamp(col('ts')))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda ts: datetime.fromtimestamp(ts), DateType())
    df = df.withColumn('date', get_datetime(col('timestamp')))

    # extract columns to create time table
    df = df \
        .withColumn('hour', hour(col('date'))) \
        .withColumn('day', dayofmonth(col('date'))) \
        .withColumn('week',weekofyear(col('date')))  \
        .withColumn('month', month(col('date'))) \
        .withColumn('year', year(col('date'))) \
        .withColumn('weekday',date_format(col('date'), 'EEEE'))
    
    time_table = df.selectExpr('timestamp as start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday').dropDuplicates()

    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + 'time_table')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(song_df, (df.song == song_df.title) & (df.artist == song_df.artist_name) & (df.length == song_df.duration), 'left') \
        .selectExpr('timestamp as start_time', 'userId as user_id', 'level','song_id', 'artist_id', 'sessionId as session_id', 
                    'location', 'userAgent as user_agent', 'year(date) as year', 'month(date) as month')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + 'songplays_table')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/processed/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()