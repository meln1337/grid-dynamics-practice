import pyspark.sql.types as t

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

title_basics_schema = t.StructType([
    t.StructField('tconst', t.StringType(), True),
    t.StructField('titleType', t.StringType(), True),
    t.StructField('primaryTitle', t.StringType(), True),
    t.StructField('originalTitle', t.StringType(), True),
    t.StructField('isAdult', t.BooleanType(), True),
    t.StructField('startYear', t.DateType(), True),
    t.StructField('endYear', t.DateType(), True),
    t.StructField('runtimeMinutes', t.IntegerType(), True),
    t.StructField('genres', t.StringType(), True),
])

ratings_schema = t.StructType([
    t.StructField('tconst', t.StringType(), True),
    t.StructField('averageRating', t.FloatType(), True),
    t.StructField('numVotes', t.IntegerType(), True)
])

episode_schema = t.StructType([
    t.StructField('tconst', t.StringType(), True),
    t.StructField('parentTconst', t.StringType(), True),
    t.StructField('seasonNumber', t.IntegerType(), True),
    t.StructField('episodeNumber', t.IntegerType(), True),
])