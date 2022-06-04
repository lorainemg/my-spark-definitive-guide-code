# Let’s create a DataFrame with which we can work
df = spark.read.format("json").load("/data/flight-data/json/2015-summary.json")


# COMMAND ----------
# Automatically infers the schema of a JSON dataset.
spark.read.format("json").load("/data/flight-data/json/2015-summary.json").schema


# COMMAND ----------
# Crear y forzar un esquema en un DataFrame
from pyspark.sql.types import StructField, StructType, StringType, LongType

myManualSchema = StructType([
  StructField("DEST_COUNTRY_NAME", StringType(), True),
  StructField("ORIGIN_COUNTRY_NAME", StringType(), True),
  StructField("count", LongType(), False, metadata={"hello":"world"})
])
df = spark.read.format("json").schema(myManualSchema)\
  .load("/data/flight-data/json/2015-summary.json")


# COMMAND ----------
# Theres different ways to construct and refer to columns but the two simplest
# way are by using col or column functions.
from pyspark.sql.functions import col, column
col("someColumnName")
column("someColumnName")


# COMMAND ----------
# To refer to a specific column in an DataFrame
df.col("count")

# COMMAND ----------
# Columns transformations using expr function
from pyspark.sql.functions import expr
expr("(((someCol + 5) * 200) - 6) < otherCol")

# COMMAND ----------
# Accessing to DataFrame's columns
spark.read.format("json").load("/data/flight-data/json/2015-summary.json").columns

# COMMAND ----------
# See a row
df.first()


# COMMAND ----------
# Creating rows
from pyspark.sql import Row
myRow = Row("Hello", None, 1, False)


# COMMAND ----------
# Accessing data in rows
myRow[0]
myRow[2]


# COMMAND ----------
# Creating a DataFrame
df = spark.read.format("json").load("/data/flight-data/json/2015-summary.json")
df.createOrReplaceTempView("dfTable")


# COMMAND ----------
# Creating a DataFrame from the fly by taking a set of rows and converting them to a DataFrame
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, StringType, LongType
myManualSchema = StructType([
  StructField("some", StringType(), True),
  StructField("col", StringType(), True),
  StructField("names", LongType(), False)
])
myRow = Row("Hello", None, 1)
myDf = spark.createDataFrame([myRow], myManualSchema)
myDf.show()


# COMMAND ----------
# Use select to pass as string the columns names we want to work with
df.select("DEST_COUNTRY_NAME").show(2)


# COMMAND ----------
# Selecting multiple columns
df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(2)


# COMMAND ----------
# Testing different ways to select columns
from pyspark.sql.functions import expr, col, column
df.select(
    expr("DEST_COUNTRY_NAME"),
    col("DEST_COUNTRY_NAME"),
    column("DEST_COUNTRY_NAME"))\
  .show(2)


# COMMAND ----------
# This changes the column name as 'destination'
df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)


# COMMAND ----------
# This changes back the column name to its original name
df.select(expr("DEST_COUNTRY_NAME as destination").alias("DEST_COUNTRY_NAME"))\
  .show(2)


# COMMAND ----------
# Because select is often used followed by a series of select expressions
# Spark has a shorthand for doing this efficiently:
df.selectExpr("DEST_COUNTRY_NAME as newColumnName", "DEST_COUNTRY_NAME").show(2)


# COMMAND ----------
# In this example adds a new column withinCountry to our DataFrame that 
# specifies whether the destination and origin are the same 
df.selectExpr(
  "*", # all original columns
  "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry")\
  .show(2)


# COMMAND ----------
# Using aggregate functions with selectExpr
df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(2)


# COMMAND ----------
# Using literals
from pyspark.sql.functions import lit
df.select(expr("*"), lit(1).alias("One")).show(2)


# COMMAND ----------
# Adds the numberOne column which has a value of 1 in all records
df.withColumn("numberOne", lit(1)).show(2)


# COMMAND ----------
# Sets a Boolean flag for when the origin country is the same as the destination country
df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME"))\
  .show(2)


# COMMAND ----------
# Renaming columns
df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").columns


# COMMAND ----------
# Here we dont need to escape characters because first argument is just a string
dfWithLongColName = df.withColumn(
    "This Long Column-Name",
    expr("ORIGIN_COUNTRY_NAME"))


# COMMAND ----------
# In this example we need to escape characters because we're referencing a column
# in a expression.
dfWithLongColName.selectExpr(
    "`This Long Column-Name`",
    "`This Long Column-Name` as `new col`")\
  .show(2)


# COMMAND ----------
# we use ` when we refer to columns names as expressions
dfWithLongColName.select(expr("`This Long Column-Name`")).columns

# COMMAND ----------
# Remove columns
df.drop("ORIGIN_COUNTRY_NAME").columns

# COMMAND ----------
# Remove multiple columns
dfWithLongColName.drop("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME")

# COMMAND ----------
# Casting count to long
df.withColumn("count2", col("count").cast("long"))

# COMMAND ----------
# Filtering with filter and where, both have the same output
df.filter(col("count") < 2).show(2)
df.where("count < 2").show(2)


# COMMAND ----------
# Specifying multiple AND filters (Spark executes them at the same time)
df.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") != "Croatia")\
  .show(2)


# COMMAND ----------
# Counts rows with unique ORIGIN_COUNTRY_NAME and DEST_COUNTRY_NAME
df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count()


# COMMAND ----------
# Counts rows with unique ORIGIN_COUNTRY_NAME
df.select("ORIGIN_COUNTRY_NAME").distinct().count()


# COMMAND ----------
# Getting a random sample of recored of a DataFrame
seed = 5
withReplacement = False
fraction = 0.5
df.sample(withReplacement, fraction, seed).count()


# COMMAND ----------
# Splitting a DataFrame
dataFrames = df.randomSplit([0.25, 0.75], seed)
dataFrames[0].count() > dataFrames[1].count() # False


# COMMAND ----------
# Concatening 2 DataFrames
from pyspark.sql import Row
schema = df.schema
newRows = [
  Row("New Country", "Other Country", 5L),
  Row("New Country 2", "Other Country 3", 1L)
]
parallelizedRows = spark.sparkContext.parallelize(newRows)
newDF = spark.createDataFrame(parallelizedRows, schema)


# COMMAND ----------
# Concatening 2 DataFrames
df.union(newDF)\
  .where("count = 1")\
  .where(col("ORIGIN_COUNTRY_NAME") != "United States")\
  .show()


# COMMAND ----------
# Sorting with sort and orderBy
df.sort("count").show(5)
df.orderBy("count", "DEST_COUNTRY_NAME").show(5)
df.orderBy(col("count"), col("DEST_COUNTRY_NAME")).show(5)


# COMMAND ----------
# Sorting descending
from pyspark.sql.functions import desc, asc
df.orderBy(expr("count desc")).show(2)
df.orderBy(col("count").desc(), col("DEST_COUNTRY_NAME").asc()).show(2)


# COMMAND ----------
# For optimization purposes, it’s sometimes advisable to sort within each partition 
# before another set of transformations. 
spark.read.format("json").load("/data/flight-data/json/*-summary.json")\
  .sortWithinPartitions("count")


# COMMAND ----------
# Using Limit to restrict what you extract
df.limit(5).show()


# COMMAND ----------

df.orderBy(expr("count desc")).limit(6).show()


# COMMAND ----------
# Por defecto existe solo una particion
df.rdd.getNumPartitions() # 1


# COMMAND ----------
# Particionando los datos en 5
df.repartition(5)


# COMMAND ----------
# Particionando basados en una columna
df.repartition(col("DEST_COUNTRY_NAME"))


# COMMAND ----------
# Particionando basados en una columna, especificando la cantidad de particiones
df.repartition(5, col("DEST_COUNTRY_NAME"))


# COMMAND ----------
# This operation will shuffle your data into five partitions based on the destination 
# country name, and then coalesce them (without a full shuffle):
df.repartition(5, col("DEST_COUNTRY_NAME")).coalesce(2)


# COMMAND ----------
# collect some of your data to the driver in order to manipulate it on
# your local machine
collectDF = df.limit(10)
collectDF.take(5) # take works with an Integer count
collectDF.show() # this prints it out nicely
collectDF.show(5, False)
collectDF.collect()


# COMMAND ----------

