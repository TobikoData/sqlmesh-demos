import typing as t
from datetime import datetime

import pandas as pd
from pyspark.sql import DataFrame, functions

from sqlmesh import ExecutionContext, model
from sqlglot.expressions import to_column


@model(
    "demo.pyspark_amount_by_status",
    cron="@daily",
    grain="status",
    audits=[
        ("unique_values", {"columns": [to_column("status")]}),
        ("not_null", {"columns": [to_column("status")]}),
    ],
    columns={
        "status": "text",
        "total_amount": "float",
    },
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> DataFrame:
    # get the upstream model's name and register it as a dependency
    table = context.table("demo.orders")

    # use spark to group by status and compute the total amount
    df = context.spark.table(table).filter("status IS NOT NULL").groupBy("status").agg(functions.sum("amount").alias("total_amount"))

    # returns the pyspark DataFrame directly, so no data is computed locally
    return df

# sqlmesh create_test demo.pyspark_amount_by_status --query demo.orders "select * from demo.orders limit 5"

# this requires databricks connect for this pyspark dataframe to work properly