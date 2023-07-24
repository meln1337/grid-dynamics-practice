import pytest
from pyspark import SparkConf
from pyspark.sql import SparkSession, Row
import pyspark.sql.types as t
import pyspark.sql.functions as f
from chispa.dataframe_comparer import assert_df_equality

spark = (
    SparkSession.builder
        .master('local')
        .appName('project')
        .config(conf=SparkConf())
        .getOrCreate()
)


df = spark.createDataFrame(
    [
        (1, "foo"),  # create your data here, be consistent in the types.
        (2, "bar"),
    ],
    ["id", "label"]  # add your column names here
)

df.printSchema()

df.show()