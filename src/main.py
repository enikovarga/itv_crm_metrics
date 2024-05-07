import glob
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import input_file_name


# note: in production, session should be created in master mode and appropriate config should be defined
def create_spark_session() -> SparkSession:
    """
    Function to create and configure a Spark session

    :return: spark session
    """
    spark = SparkSession.builder.master("local").appName("CRMReports").getOrCreate()
    return spark


# note: replace this with function to list files in S3 in production
def get_files(input_file_path: str) -> list[str]:
    """
    Function to list all files in the provided path/prefix.

    :param input_file_path: file path with file name prefix
    :return: list of filenames
    """
    return glob.glob(f"{input_file_path}_*")


# note: there should be an incremental process implemented to only pickup new files for processing
def read_json_files(
    spark: SparkSession,
    path: str,
) -> DataFrame:
    """
    Function to create a spark dataframe from json files.

    :param spark: spark session
    :param path: file path and file name prefix
    :return: spark dataframe
    """
    files = get_files(path)
    df = spark.read.options(multiLine=True).json(files)
    df = df.withColumn("filename", input_file_name())
    return df


# note: choosing coalesce 1 as default, in production this should be reviewed and amended based on data size
def write_parquet_files(
    df: DataFrame, output_path: str, mode: str = "append", partition_by: str = None
):
    """
    Function to write a spark dataframe as parquet files.

    :param df: spark dataframe
    :param output_path: output file path
    :param mode: write mode of append or overwrite
    :param partition_by: field name to use for partitioning the output
    """
    if partition_by:
        df.coalesce(1).write.mode(mode).partitionBy(partition_by).parquet(output_path)
    else:
        df.coalesce(1).write.mode(mode).parquet(output_path)


# note: need to confirm whether each extract file contains full historical data
def campaign_data_raw(spark: SparkSession, input_path: str, output_path: str):
    """
    Function to read campaign json files, standardise format and write as parquet.

    :param spark: spark session
    :param input_path: input file path
    :param output_path: output file path
    """
    df = read_json_files(spark, input_path)
    df.createOrReplaceTempView("campaign_raw")
    df = spark.sql(
        """
            SELECT id AS campaign_id,
                   details.name AS campaign_name,
                   details.schedule[0] AS start_date,
                   details.schedule[1] AS end_date,
                   steps,
                   filename,
                   regexp_extract(filename, '([0-9]{14})', 1) AS file_date
            FROM campaign_raw
        """
    )

    write_parquet_files(
        df=df, output_path=output_path, mode="append", partition_by="file_date"
    )


# note: need to confirm whether each extract file contains full historical data
def user_data_raw(spark: SparkSession, input_path: str, output_path: str):
    """
    Function to read user engagement json files, standardise format and write as parquet.

    :param spark: spark session
    :param input_path: input file path
    :param output_path: output file path
    """
    df = read_json_files(spark, input_path)
    df.createOrReplaceTempView("user_engagement_raw")
    df = spark.sql(
        """
            SELECT userId AS user_id,
                   campaign AS campaign_id,
                   eventTimestamp AS event_timestamp,
                   action,
                   filename
            FROM user_engagement_raw
        """
    )

    write_parquet_files(
        df=df, output_path=output_path, mode="append", partition_by="campaign_id"
    )


def create_campaign_overview_report(
    spark: SparkSession, input_path: str, output_path: str
):
    """
    Function to read the parquet campaign data and generate a de-duplicated reporting view containing all campaigns

    :param spark: spark session
    :param input_path: input file path
    :param output_path: output file path
    """
    df = spark.read.parquet(input_path)
    df.createOrReplaceTempView("campaign")

    df = spark.sql(
        """
            SELECT campaign_id,
                   campaign_name,
                   number_of_steps,
                   start_date,
                   end_date
            FROM
                (SELECT campaign_id,
                       campaign_name,
                       SIZE(steps) AS number_of_steps,
                       start_date,
                       end_date,
                       ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY file_date DESC) AS row_num
                FROM campaign)
            WHERE row_num = 1
            ORDER BY start_date
        """
    )

    write_parquet_files(df=df, output_path=output_path, mode="overwrite")


# note: formula used for avg completion rate: total number of opened messaged / total number of successfully delivered messages
# TODO: confirm with business formula + how to handle multiple messages sent to the same user under a single campaign
def create_current_campaign_engagement_report(
    spark: SparkSession, input_path: str, output_path: str
):
    """
    Function to combine campaign and user engagement datasets to generate a user engagement report for currently live campaigns.

    :param spark: spark session
    :param input_path: input file path
    :param output_path: output file path
    """
    current_date = datetime.now().strftime("%Y-%m-%d")
    df = spark.read.parquet(input_path)
    df.createOrReplaceTempView("user_engagement")

    # de-duplicating user_engagement data on event_timestamp
    # excluding campaigns completed
    spark.sql(
        f"""
        SELECT campaign_name,
               user_id,
               event_timestamp,
               action
        FROM
            (SELECT
                   c.campaign_name,
                   ue.user_id,
                   ue.event_timestamp,
                   ue.action,
                   ROW_NUMBER() OVER (PARTITION BY ue.campaign_id, ue.user_id, ue.event_timestamp ORDER BY ue.filename DESC) AS row_num
            FROM campaign c
            LEFT JOIN user_engagement ue
            ON ue.campaign_id = c.campaign_id
            WHERE c.end_date >= '{current_date}'
            )
        WHERE row_num = 1
    """
    ).createOrReplaceTempView("user_engagement_clean")

    df = spark.sql(
        """
            SELECT campaign_name,
                   COALESCE(average_percent_completion, 0) AS average_percent_completion,
                   RANK() OVER (ORDER BY average_percent_completion DESC) AS rank
            FROM
                (SELECT campaign_name,
                       ROUND(SUM(CASE WHEN action = 'MESSAGE_OPENED' THEN 1 ELSE 0 END) /
                       SUM(CASE WHEN action <> 'DELIVERY_FAILED' THEN 1 ELSE 0 END), 2) AS average_percent_completion
                FROM  user_engagement_clean
                GROUP BY campaign_name)
            ORDER BY rank
        """
    )

    write_parquet_files(df=df, output_path=output_path, mode="overwrite")


def main():
    spark = create_spark_session()

    campaign_data_raw(
        spark,
        input_path="mock_s3_bucket/input/crm_campaign",
        output_path="mock_s3_bucket/internal/campaign",
    )
    user_data_raw(
        spark,
        input_path="mock_s3_bucket/input/crm_user_engagement",
        output_path="mock_s3_bucket/internal/user_engagement",
    )

    create_campaign_overview_report(
        spark,
        input_path="mock_s3_bucket/internal/campaign",
        output_path="mock_s3_bucket/output/campaign_overview",
    )
    create_current_campaign_engagement_report(
        spark,
        input_path="mock_s3_bucket/internal/user_engagement",
        output_path="mock_s3_bucket/output/current_campaign_engagement",
    )


if __name__ == "__main__":
    main()
