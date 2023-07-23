import pytest
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t
import pyspark.sql.functions as f

path = 'C:/files/grid-dynamics'

akas_schema = t.StructType([
    t.StructField('titleId', t.StringType(), True),
    t.StructField('ordering', t.IntegerType(), True),
    t.StructField('title', t.StringType(), True),
    t.StructField('region', t.StringType(), True),
    t.StructField('language', t.StringType(), True),
    t.StructField('types', t.StringType(), True),
    t.StructField('attributes', t.StringType(), True),
    t.StructField('isOriginalTitle', t.BooleanType(), True)
])




# Create a SparkSession
spark = (
    SparkSession.builder
        .master('local')
        .appName('project')
        .config(conf=SparkConf())
        .getOrCreate()
)

akas = spark.read.csv(f'{path}/title.akas.tsv',
                                  sep=r'\t',
                                  header=True,
                                  nullValue='null',
                                  schema=akas_schema)

def test_akas_dataframe_load():
    # Load the DataFrame from the file
    # Assert that the DataFrame is not empty
    assert akas.count() > 0, "DataFrame 'akas' is empty."

def test_akas_dataframe_load_2():
    # Load the DataFrame from the file
    # Assert that the DataFrame is not empty
    assert akas.count() > 1, "DataFrame 'akas' is empty."