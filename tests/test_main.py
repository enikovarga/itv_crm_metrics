import pytest
from freezegun import freeze_time
from pyspark.sql import Row

from src.main import (
    campaign_data_raw,
    create_campaign_overview_report,
    create_current_campaign_engagement_report,
    get_files,
    user_data_raw,
)


@pytest.mark.parametrize(
    "input_path, expected_files",
    [
        ("tests/data/crm_campaign", ["tests/data/crm_campaign_20230101001500.json"]),
        (
            "tests/data/crm_user_engagement",
            ["tests/data/crm_user_engagement_20230101001500.json"],
        ),
    ],
)
def test_get_files(input_path, expected_files):
    output = get_files(input_path)
    assert output == expected_files


def test_campaign_data_raw(spark, tmp_dir):
    campaign_data_raw(spark, input_path="tests/data/crm_campaign", output_path=tmp_dir)

    output = spark.read.parquet(tmp_dir)

    assert output.columns == [
        "campaign_id",
        "campaign_name",
        "start_date",
        "end_date",
        "steps",
        "filename",
        "file_date",
    ]

    assert output.count() == 2

    campaign1 = output.select(
        "campaign_id", "campaign_name", "start_date", "end_date"
    ).collect()[0]
    campaign2 = output.select(
        "campaign_id", "campaign_name", "start_date", "end_date"
    ).collect()[1]

    assert campaign1 == Row(
        campaign_id="6fg7e8",
        campaign_name="summer_romance_binge",
        start_date="2023-07-21",
        end_date="2023-07-31",
    )
    assert campaign2 == Row(
        campaign_id="cb571",
        campaign_name="win_back",
        start_date="2023-07-01",
        end_date="2023-07-25",
    )


def test_user_data_raw(spark, tmp_dir):
    user_data_raw(
        spark, input_path="tests/data/crm_user_engagement", output_path=tmp_dir
    )

    output = spark.read.parquet(tmp_dir)

    assert output.columns == [
        "user_id",
        "event_timestamp",
        "action",
        "filename",
        "campaign_id",
    ]

    assert output.count() == 4

    engagement_row1 = output.drop("filename").collect()[0]
    engagement_row2 = output.drop("filename").collect()[1]

    assert engagement_row1 == Row(
        user_id="e4fa9a65-692b-4ecc-bff9-502f5480d555",
        event_timestamp="2023-07-11T09:08:19.994Z",
        action="MESSAGE_DELIVERED",
        campaign_id="6fg7e8",
    )
    assert engagement_row2 == Row(
        user_id="e4fa9a65-692b-4ecc-bff9-502f5480d555",
        event_timestamp="2023-07-13T17:18:00.101Z",
        action="MESSAGE_OPENED",
        campaign_id="6fg7e8",
    )


def test_create_campaign_overview_report(spark, tmp_dir):
    # creating test campaign parquet dataset
    campaign_data_raw(
        spark, input_path="tests/data/crm_campaign", output_path=f"{tmp_dir}/campaign"
    )
    create_campaign_overview_report(
        spark,
        input_path=f"{tmp_dir}/campaign",
        output_path=f"{tmp_dir}/campaign_overview_report",
    )

    # validating output
    output = spark.read.parquet(f"{tmp_dir}/campaign_overview_report")

    print(output.columns)
    assert output.columns == [
        "campaign_id",
        "campaign_name",
        "number_of_steps",
        "start_date",
        "end_date",
    ]

    campaign1 = output.collect()[0]
    campaign2 = output.collect()[1]

    assert campaign1 == Row(
        campaign_id="cb571",
        campaign_name="win_back",
        number_of_steps=3,
        start_date="2023-07-01",
        end_date="2023-07-25",
    )
    assert campaign2 == Row(
        campaign_id="6fg7e8",
        campaign_name="summer_romance_binge",
        number_of_steps=4,
        start_date="2023-07-21",
        end_date="2023-07-31",
    )


@freeze_time("2023-07-25")
def test_create_current_campaign_engagement_report(spark, tmp_dir):
    # creating test campaign parquet dataset
    campaign_data_raw(
        spark, input_path="tests/data/crm_campaign", output_path=f"{tmp_dir}/campaign"
    )
    spark.read.parquet(f"{tmp_dir}/campaign").createOrReplaceTempView("campaign")

    # creating test user engagement parquet dataset
    user_data_raw(
        spark,
        input_path="tests/data/crm_user_engagement",
        output_path=f"{tmp_dir}/user_engagement",
    )

    # running report generation function
    create_current_campaign_engagement_report(
        spark, f"{tmp_dir}/user_engagement", f"{tmp_dir}/campaign_engagement_report"
    )

    # validating output
    output = spark.read.parquet(f"{tmp_dir}/campaign_engagement_report")

    assert output.columns == ["campaign_name", "average_percent_completion", "rank"]

    row1 = output.collect()[0]
    row2 = output.collect()[1]

    assert row1 == Row(
        campaign_name="summer_romance_binge", average_percent_completion=0.33, rank=1
    )
    assert row2 == Row(campaign_name="win_back", average_percent_completion=0.0, rank=2)
