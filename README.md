# Data Analysis Using PySpark 

Adapted from: [Data Analysis Using PySpark | Coursera](coursera.org/learn/data-analysis-using-pyspark/)

Project structure:
```
1. Intro
2. Importing data
3. Run queries
4. Import more data
5. Visualise
```

Go to colab.research.google.com

Mount the google drive:
```python
from google.colab import drive
drive.mount("/content/drive")
```

Install `pyspark` module:
```python
!pip install pyspark
```

Import modules:
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, desc, col, max
import matplotlib.pyplot as plts
```

Create a spark session:
```python
spark = SparkSession.builder.appName("spark_app").getOrCreate()
```

Import the data:
```python
listening_csv_path = "/content/drive/My Drive/dataset/listenings.csv"
listening_df = spark.read.format("csv")\
	.option("inferSchema", True)\
	.option("header", True)\
	.load(listening_csv_path)
```

Check the data:
```python
listening_df.show()
```

Delete useless columns:
```python
listening_df = listening_df.drop("date")
```

Drop null rows:
```python
listening_df = listening_df.na.drop()
```

Check data again:
```python
listening_df.show()
```

Check the schema:
```python
listening_df.printSchema()
```

Check dataframe shape:
```python
shape = ( listening_df.count() , len(listening_df.columns) )
print(shape)
```

Query data:
```python
# select track and artist columns
q0 = listening_df.select("artisit", "track")

# find all records where users listened to Rihanna
q1 = listening_df\
	.select("*")\
	.filter(listening_df.artist == "Rihanna")

# find top 10 users that are fans of Rihanna
q2 = listening_df.select("user_id)\
	.select("user_id")\
	.filter(listening_df == "Rihanna")\
	.groupby("user_id")\
	.agg( count("user_id").alias("count") )\
	.orderBy( decs("count") )\
	.limit(10)

# find top 10 famous tracks
# note: multiple artists may have the same track name
q3 = listening_df\
	.select("artist", "track")\
	.groupby("artist", "track")\
	.agg( count("*").alias("count") )\
	.limit(10)

# find top 10 famous tracks of Rihanna
q4 = listening_df\
	.select("artist", "track")\
	.filter(listening_df == "Rihanna")\
	.groupby("artisit", "track")\
	.agg( count("*").alias("count") )\
	.orderBy( desc("count") )\
	.limit(10)

# find top 10 famous albums
q5 = listening_df\
	.select("artist", "album")\
	.groupby("artist", "album")\
	.agg( count("*").alias("count") )\
	.orderBy( desc("count") )\
	.limit(10)
```

Import another file:
```python
genre_csv_path = "..."
genre_df = spark\
	.read\
	.format("csv")\
	.option("inferSchmea", True)\
	.option("header", True)\
	.load(genre_csv_path)
```

Check the data:
```python
genre_df.show()
```

Join the two datasets:
```python
data = listening_df.join(genre_df, how="inner", on=["artist"])
data.show()
```

Query data:
```python
# find top 10 users that are fans of pop music
q6 = data\
	.select("user_id")\
	.filter(data.genre == "pop")\
	.groupby("user_id")\
	.agg( count("*").alias("count") )\
	.orderBy( desc("count") )\
	.limit(10)

# find top 10 famous genres
q7 = data\
	.select("genre")\
	.groupby("genre")'\
	.agg( count("*").alias("count") )\
	.orderBy( desc("count") )\
	.limit(10)

# find out each user's favourite genre
from pyspark.sql.functions import struct
q8_1 = data\
	.select( "user_id", "genre" )\
	.groupby( "user_id", "genre" )\
	.agg( count("*").alias("count") )\
	.orderBy( "user_id" )

q8_2 = q8_1\
	.groupby( "user_id" )\
	.agg(
		max(
			struct( col("count"), col("genre") )
		).alias("max")
	)\
	.select( col("user_id"), col("max.genre") )

# find out how many pop, rock, metal, and hip hop singers we have
q9 = genre_df\
	.select( "genre" )\
	.filter(
		( col("genre")=="pop" )
		| ( col("genre")=="rock" )
		| ( col("genre")=="metal" )
		| ( col("genre")=="hip hop" )
	)\
	.groupby( col("genre") )\
	.agg( count("genre").alias("count") )
```

Visualise results using `matplotlib`:
```python
q9_list = q9.collect()
labels = [row["genre"] for row in q9_list]
counts = [row["count"] for row in q9_list]
```

Visualise these two lists using a bar chart:
```python
plts.bar(labels, counts)
```