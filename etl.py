import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek

# Set spark environments
# os.environ['PYSPARK_PYTHON'] = 'C:\spark\spark-3.2.1-bin-hadoop3.2'
# os.environ['PYSPARK_DRIVER_PYTHON'] = 'C:\spark\spark-3.2.1-bin-hadoop3.2'

config = configparser.ConfigParser()
config.read('dl.cfg')
os.environ['AWS_ACCESS_KEY_ID']=config.get("config","AWS_ACCESS_KEY_ID")
os.environ['AWS_SECRET_ACCESS_KEY']=config.get("config","AWS_SECRET_ACCESS_KEY")
os.environ['AWS_SESSION_TOKEN']=config.get("config","AWS_SESSION_TOKEN")

def create_spark_session():
    """
        Create Spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
        - Read song data from into
        - Extract data into songs, artist table as dataframe
        - Write all dataframe into parquet files in folder 'data_output'
    """
    # get filepath to song data file
    # song_data = os.path.join(input_data, 'song-data', '*', '*', '*')
    song_data = input_data + 'song_data/*/*/*/*.json'
    df = spark.read.json(song_data)
    # extract columns to create songs table
    songs_table = df.select(
        ['song_id', 'title', 'artist_id', 'year', 'duration']).dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    # songs_table.write.partitionBy("year", "artist_id").mode(
    #     "overwrite").parquet(os.path.join(output_data, 'songs'))
    songs_table.write.partitionBy("year", "artist_id").mode(
        "overwrite").parquet(output_data+ 'songs/')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location',
                              'artist_latitude', 'artist_longitude') \
        .withColumnRenamed('artist_name', 'artist') \
        .withColumnRenamed('artist_location', 'location') \
        .withColumnRenamed('artist_latitude', 'latitude') \
        .withColumnRenamed('artist_longitude', 'longitude').dropDuplicates()

    # write artists table to parquet files
    # artists_table.write.mode("overwrite").parquet(
    #     os.path.join(output_data, 'artists'))
    artists_table.write.mode("overwrite").parquet(output_data+ 'artists/')


def process_log_data(spark, input_data, output_data):
    """
        - Read log data from into
        - Extract data into users, time and songplays table as dataframe
        - Write all dataframe into parquet files in folder 'data_output'
    """
    # get filepath to log data file
    # log_data = os.path.join(input_data, 'log-data', '*')
    log_data = input_data + 'log_data/*/*/*events.json'
#     log_data = "input_data/log-data/*/*/*events.json"
    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table
    users_table = df.select(
        ['userId', 'firstName', 'lastName', 'gender', 'level']) \
        .withColumnRenamed('userId', 'user_id')\
        .withColumnRenamed('firstName', 'first_name')\
        .withColumnRenamed('lastName', 'last_name').dropDuplicates()

    # write users table to parquet files
    # users_table.write.mode("overwrite").parquet(
    #     os.path.join(output_data, 'users'))
    users_table.write.mode("overwrite").parquet(output_data, 'users')

    # create datetime column from original timestamp column
    get_datetime = udf(lambda timestamp: datetime.fromtimestamp(
        timestamp/1000).isoformat())
    df = df.withColumn('datetime', get_datetime(df.ts))

    # extract columns to create time table
    time_table = df.select('datetime').withColumn('start_time', df.datetime) \
        .withColumn('hour', hour('datetime')) \
        .withColumn('day', dayofmonth('datetime')) \
        .withColumn('week', weekofyear('datetime')) \
        .withColumn('month', month('datetime')) \
        .withColumn('year', year('datetime')) \
        .withColumn('weekday', dayofweek('datetime')).dropDuplicates()

    # write time table to parquet files partitioned by year and month
    # time_table.write.mode("overwrite").partitionBy("year", "month") \
    #     .parquet(os.path.join(output_data, 'time'))
    time_table.write.mode("overwrite").partitionBy("year", "month") \
        .parquet(output_data+'time/')

    # read in song data to use for songplays table
    # song_df = spark.read.json(os.path.join(
    #     input_data, 'song-data', '*', '*', '*'))
    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')

    # extract columns from joined song and log datasets to create songplays table
    """
        Create temp view :
        song_df as songs
        df as logs
        Use sql to join temp table
    """
    song_df.createOrReplaceTempView('songs')
    # create a monotonically increasing songplay_id 
    df = df.withColumn('songplay_id', F.monotonically_increasing_id())
    df.createOrReplaceTempView('logs')
    songplays_table = spark.sql("""
        SELECT
            l.songplay_id,
            l.datetime as start_time,
            year(l.datetime) as year,
            month(l.datetime) as month,
            l.userId as user_id,
            l.level,
            s.song_id,
            s.artist_id,
            l.sessionId as session_id,
            l.location,
            l.userAgent as user_agent
        FROM logs l
        LEFT JOIN songs s ON
            l.song = s.title AND
            l.artist = s.artist_name 
    """)

    # write songplays table to parquet files partitioned by year and month
    # songplays_table.write.partitionBy("year", "month").mode("overwrite").parquet(
    #     os.path.join(output_data, 'songplays'))
    songplays_table.write.partitionBy("year", "month").mode("overwrite") \
    .parquet(output_data+'songplays/')


def main():
    """
        The point of execution for this file
    """
    spark = create_spark_session()
    # for aws env
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend-namhh/output/"
    # input_data = "s3a://udacity-dend-namhh4/input/"
    # output_data = "s3a://udacity-dend-namhh/output/"
    # for local
    # input_data = "data"
    # output_data = "data_output"
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
