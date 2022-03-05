import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F

# Load the configuration file if the env variable is not set
from pyspark.sql.types import TimestampType, IntegerType

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
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')

    # read song data file
    df = spark.read.options(inferSchema='true').json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(output_data + 'songs/')

    # extract columns to create artists table
    artists_table = df.select('artist_id',
                              F.col('artist_name').alias('name'),
                              F.col('artist_location').alias('location'),
                              F.col('artist_latitude').alias('latitude'),
                              F.col('artist_longitude').alias('longitude'))

    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + 'artists/')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*.json')

    # read log data file
    df = spark.read.options(inferSchema='true').json(log_data)

    # filter by actions for song plays
    df = df.filter(F.col('page') == 'NextSong')

    # extract columns for users table
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level', 'ts')

    # filter only by latest ts for the user
    window_spec = Window.partitionBy('userID')
    users_table = users_table.withColumn('max_ts_user', F.max(F.col('ts')).over(window_spec))
    users_table = users_table.filter(F.col('max_ts_user') == F.col('ts'))

    # dropping and renaming columns
    users_table = users_table.drop('ts', 'max_ts_user') \
        .withColumnRenamed('userId', 'user_id') \
        .withColumnRenamed('firstName', 'first_name') \
        .withColumnRenamed('lastName', 'last_name') \
        .withColumnRenamed('firstName', 'first_name')

    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + 'users/')

    # create timestamp column from original timestamp column
    get_timestamp = F.udf(lambda x: datetime.fromtimestamp(x / 1000.0), TimestampType())
    df = df.withColumn('start_time', get_timestamp(F.col('ts')))

    # extract columns to create time table
    time_table = df \
        .withColumn('hour', F.hour('start_time')) \
        .withColumn('day', F.dayofmonth('start_time')) \
        .withColumn('week', F.weekofyear('start_time')) \
        .withColumn('month', F.month('start_time')) \
        .withColumn('year', F.year('start_time')) \
        .withColumn('weekday', F.dayofweek('start_time')) \
        .select('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday') \
        .distinct()

    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'time/')

    # read in song data and artist to use for songplays table
    songs_df = spark.read.parquet(output_data + '/songs')
    artists_df = spark.read.parquet(output_data + '/artists')

    # join song and artist data
    songs_details = songs_df.join(artists_df, 'artist_id', 'full') \
        .select('song_id', 'title', 'artist_id', 'name', 'year')

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(songs_details, (df.song == songs_details.title) & (df.artist == songs_details.name),
                              'left')

    # convert ts to timestamp
    songplays_table = songplays_table.withColumn('start_time', get_timestamp(F.col('ts')))

    songplays_table = songplays_table \
        .withColumn('user_id', F.col('userId').cast(IntegerType())) \
        .withColumn('month', F.month('start_time')) \
        .withColumn('year', F.year('start_time')) \
        .select('start_time',
                'user_id',
                'level',
                'song_id',
                'artist_id',
                'sessionId',
                'location',
                'year',
                'month',
                F.col('userAgent').alias('user_agent')) \
        .withColumn('songplay_id', F.monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'songplays/')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://veectro-udacity-dend4/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
