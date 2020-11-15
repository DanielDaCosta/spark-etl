import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
import pyspark.sql.functions as F
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*"

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id, title, artist_id, year, duration').\
        dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(
        output_data + 'songs/songs.parquet',
        'overwrite'
    )

    # renaming columns
    df = df.withColumnRenamed('artist_name', 'name').\
        withColumnRenamed('artist_location', 'location').\
        withColumnRenamed('artist_latitude', 'latitude').\
        withColumnRenamed('artist_longitude', 'longitude').\
        dropDuplicates()

    # write artists table to parquet files
    artists_table.createOrReplaceTempView('artists')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location',
                              'artist_latitude', 'artist_longitude')

    artists_table.write.parquet(
        output_data + 'artists/artists.parquet',
        'overwrite'
    )


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "log_data/*"

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df_actions = df.filter(df.page == 'NextSong').\
        select('ts', 'userId', 'level', 'song', 'artist',
               'sessionId', 'location', 'userAgent')

    # extract columns for users table
    users_table = df.select('userId', 'firstName', 'lastName',
                            'gender', 'level').dropDuplicates()

    # write users table to parquet files
    users_table.createOrReplaceTempView('users')
    users_table.write.parquet(
        output_data + 'users/users.parquet',
        'overwrite'
    )

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: datetime.datetime.fromtimestamp(x / 1000),
                        TimestampType())
    df_actions = df_actions.withColumn(
        'timestamp',
        get_timestamp(df_actions.ts)
    )

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: F.to_date(x), TimestampType())
    df_actions = df_actions.withColumn(
        'start_time',
        get_datetime(df_actions.ts)
    )

    # extract columns to create time table
    time_table = actions_df.select('datetime') \
                        .withColumn('start_time', actions_df.datetime) \
                        .withColumn('hour', hour('datetime')) \
                        .withColumn('day', dayofmonth('datetime')) \
                        .withColumn('week', weekofyear('datetime')) \
                        .withColumn('month', month('datetime')) \
                        .withColumn('year', year('datetime')) \
                        .withColumn('weekday', dayofweek('datetime')) \
                        .dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table

    # read in song data to use for songplays table
    song_df = 

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = 

    # write songplays table to parquet files partitioned by year and month
    songplays_table


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
