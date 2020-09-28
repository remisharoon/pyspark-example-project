"""
etl_job.py
~~~~~~~~~~

    $SPARK_HOME/bin/spark-submit \
    --master spark://localhost:7077 \
    --py-files packages.zip \
    --files configs/etl_config.json \
    jobs/etl_job.py

"""

import pyspark.sql.functions as f
from pyspark.sql import Row
from pyspark.sql.functions import col, concat_ws, lit, lag, unix_timestamp, when
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType

from dependencies.spark import start_spark

def main():
    """Main ETL script definition.

    :return: None
    """
    # start Spark application and get Spark session, logger and config
    spark, log, config = start_spark(
        app_name='my_etl_job',
        files=['configs/etl_config.json'])

    # log that main ETL job is starting
    log.warn('etl_job is up-and-running')

    # execute ETL pipeline
    user_df, user_playback_df = extract_data(spark)

    # print(user_df.show(10))

    # print(user_playback_df.show(10))

    user_playback_df = user_playback_df.repartition("userid") \
        .withColumn("timestamp", unix_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'").cast(TimestampType()))  # 2006-05-09T22:12:52Z

    distinct_songs_by_user = user_playback_df.groupby("userid", "track-name").count().alias("play_count").sort(col("count").desc())
    print(distinct_songs_by_user.show(10))




    # log the success and terminate Spark application
    log.warn('test_etl_job is finished')
    spark.stop()
    return None


def extract_data(spark):
    """Load data from Parquet file format.

    :param spark: Spark session object.
    :return: Spark DataFrame.
    """
    user_df = (
        spark
        .read
        .option("delimiter", "\t")
        .option("header", True)
        .csv('../data/lastfm-dataset-1K/userid-profile.tsv'))

    user_playback_df = (
        spark
        .read
        .option("delimiter", "\t")
        .option("header", False)
        .csv('../data/lastfm-dataset-1K/userid-timestamp-artid-artname-traid-traname.tsv')
        .toDF("userid","timestamp","musicbrainz-artist-id","artist-name","musicbrainz-track-id","track-name")
    )


    # user_playback_df.columns = ["userid","timestamp","musicbrainz-artist-id","artist-name","musicbrainz-track-id","track-name"]

    return user_df, user_playback_df


def transform_data(df, steps_per_floor_):
    """Transform original dataset.

    :param df: Input DataFrame.
    :param steps_per_floor_: The number of steps per-floor at 43 Tanner
        Street.
    :return: Transformed DataFrame.
    """
    df_transformed = (
        df
        .select(
            col('id'),
            concat_ws(
                ' ',
                col('first_name'),
                col('second_name')).alias('name'),
               (col('floor') * lit(steps_per_floor_)).alias('steps_to_desk')))

    return df_transformed


def load_data(df):
    """Collect data locally and write to CSV.

    :param df: DataFrame to print.
    :return: None
    """
    (df
     .coalesce(1)
     .write
     .csv('loaded_data', mode='overwrite', header=True))
    return None


def create_test_data(spark, config):
    """Create test data.

    This function creates both both pre- and post- transformation data
    saved as Parquet files in tests/test_data. This will be used for
    unit tests as well as to load as part of the example ETL job.
    :return: None
    """
    # create example data from scratch
    local_records = [
        Row(id=1, first_name='Dan', second_name='Germain', floor=1),
        Row(id=2, first_name='Dan', second_name='Sommerville', floor=1),
        Row(id=3, first_name='Alex', second_name='Ioannides', floor=2),
        Row(id=4, first_name='Ken', second_name='Lai', floor=2),
        Row(id=5, first_name='Stu', second_name='White', floor=3),
        Row(id=6, first_name='Mark', second_name='Sweeting', floor=3),
        Row(id=7, first_name='Phil', second_name='Bird', floor=4),
        Row(id=8, first_name='Kim', second_name='Suter', floor=4)
    ]

    df = spark.createDataFrame(local_records)

    # write to Parquet file format
    (df
     .coalesce(1)
     .write
     .parquet('tests/test_data/employees', mode='overwrite'))

    # create transformed version of data
    df_tf = transform_data(df, config['steps_per_floor'])

    # write transformed version of data to Parquet
    (df_tf
     .coalesce(1)
     .write
     .parquet('tests/test_data/employees_report', mode='overwrite'))

    return None


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
