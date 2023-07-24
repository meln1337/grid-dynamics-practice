import pyspark.sql
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from chispa.dataframe_comparer import assert_df_equality
from schemas import akas_schema, ratings_schema, title_basics_schema

path = 'C:/files/grid-dynamics'

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

title_basics = spark.read.csv(f'{path}/title.basics.tsv',
                                    sep=r'\t',
                                    header=True,
                                    nullValue='null',
                                    schema=title_basics_schema,
                                    dateFormat='yyyy')

ratings = spark.read.csv(f'{path}/title.ratings.tsv',
                                    sep=r'\t',
                                    header=True,
                                    nullValue='null',
                                    schema=ratings_schema)

def how_many_ua_titles(akas: pyspark.sql.DataFrame) -> int:
    return akas.filter(akas.region == 'UA').count()

def first_5_titles_short(title_basics: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    return title_basics.filter(f.col('genres').contains('Short')).limit(5)

def title_with_the_most_numvotes(title_basics: pyspark.sql.DataFrame, ratings: pyspark.sql.DataFrame) -> str:
    return title_basics.filter(f.col('genres').contains('Documentary')) \
        .select('tconst', 'primaryTitle') \
        .join(ratings, 'tconst', 'inner') \
        .sort(f.desc('numVotes')) \
        .limit(1) \
        .collect()[0]['primaryTitle']

def get_the_most_common_region(akas: pyspark.sql.DataFrame) -> str:
    return akas.groupBy('region') \
        .count() \
        .sort(f.desc('count')) \
        .collect()[0]['region']

def get_title_with_highest_rating(ratings: pyspark.sql.DataFrame, title_basics: pyspark.sql.DataFrame) -> str:
    return ratings.join(title_basics, 'tconst', 'left') \
        .select('tconst', 'originalTitle', 'averageRating') \
        .sort(f.desc('averageRating')) \
        .collect()[0]['originalTitle']

def test_1():
    expected_result = how_many_ua_titles(akas)
    assert expected_result == 27365, "There are 27365 titles with UA region"

def test_2():
    expected_result = spark.read.csv('C:/Projects/jupyter projects/practice grid dynamics/2.tsv',
                                    sep=r'\t',
                                    header=True,
                                    nullValue='null',
                                    schema=title_basics_schema)
    result = first_5_titles_short(title_basics)
    assert_df_equality(result, expected_result)

def test_3():
    result = title_with_the_most_numvotes(title_basics, ratings)
    assert result == "Planet Earth", "Wrong title"

def test_4():
    result = get_the_most_common_region(akas)
    assert result == "DE", "Wrong region"

def test_5():
    result = get_title_with_highest_rating(ratings, title_basics)
    assert result == "Die Fee", "Wrong title"