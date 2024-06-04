import typing as t
from datetime import datetime

import pandas as pd
from pyspark.sql import DataFrame, functions

from sqlmesh import ExecutionContext, model

@model(
    "demo.pyspark_amount_by_status",
    cron="@daily",
    grain="status",
    # audits={ # the audits don't work: Error: Macro variable 'columns' is undefined.
    #     "UNIQUE_VALUES": {"columns": ["status"]},
    #     "NOT_NULL": {"columns": ["status"]}
    # },
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
    df = context.spark.table(table).groupBy("status").agg(functions.sum("amount").alias("total_amount"))

    # returns the pyspark DataFrame directly, so no data is computed locally
    return df

# sqlmesh create_test demo.pyspark_amount_by_status --query demo.orders "select * from demo.orders limit 5" 
# sqlmesh create_test demo.pyspark_amount_by_status --query demo.orders "context.spark.sql("SELECT * FROM demo.orders LIMIT 5")" 