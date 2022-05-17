# --------------------- Structure Streaming ---------------------
# Analyze the data as a static dataset and create a DataFrame to do so.
staticDataFrame = spark.read.format("csv")\
  .option("header", "true")\
  .option("inferSchema", "true")\
  .load("/data/retail-data/by-day/*.csv")

staticDataFrame.createOrReplaceTempView("retail_data")
# Create a schema for this dataset
staticSchema = staticDataFrame.schema


# COMMAND ----------
# In this example we’ll take a look at the sale hours during which a
# given customer (identified by CustomerId) makes a large purchase.
from pyspark.sql.functions import window, column, desc, col
staticDataFrame\
  .selectExpr(
    "CustomerId",
    "(UnitPrice * Quantity) as total_cost",
    "InvoiceDate")\
  .groupBy(
    col("CustomerId"), window(col("InvoiceDate"), "1 day"))\
  .sum("total_cost")\
  .sort(desc("sum(total_cost)"))\
  .show(5)


# COMMAND ----------
#! Using Streaming Code
streamingDataFrame = spark.readStream\
    .schema(staticSchema)\
    .option("maxFilesPerTrigger", 1)\
    .format("csv")\
    .option("header", "true")\
    .load("/data/retail-data/by-day/*.csv")


# COMMAND ----------
# Let’s set up the same business logic as the previous DataFrame manipulation. We’ll perform a
# summation in the process. This is a lazy operation.
purchaseByCustomerPerHour = streamingDataFrame\
  .selectExpr(
    "CustomerId",
    "(UnitPrice * Quantity) as total_cost",
    "InvoiceDate")\
  .groupBy(
    col("CustomerId"), window(col("InvoiceDate"), "1 day"))\
  .sum("total_cost")


# COMMAND ----------
# memory = store in-memory table
# customer_purchases = the name of the in-memory table
# complete = all the counts should be in the table

purchaseByCustomerPerHour.writeStream\
    .format("memory")\
    .queryName("customer_purchases")\
    .outputMode("complete")\
    .start()

# COMMAND ----------
# When we start the stream, we can run queries against it to debug what our result will look like if
# we were to write this out to a production sink:
spark.sql("""
  SELECT *
  FROM customer_purchases
  ORDER BY `sum(total_cost)` DESC
  """)\
  .show(5)


# --------------------- Machine Learning Analytics ---------------------
# COMMAND ----------
from pyspark.sql.functions import date_format, col
# Transform data to numerical types
preppedDataFrame = staticDataFrame\
  .na.fill(0)\
  .withColumn("day_of_week", date_format(col("InvoiceDate"), "EEEE"))\
  .coalesce(5)


# COMMAND ----------
# Split train and test set
trainDataFrame = preppedDataFrame\
  .where("InvoiceDate < '2011-07-01'")
testDataFrame = preppedDataFrame\
  .where("InvoiceDate >= '2011-07-01'")


# COMMAND ----------
# Using some transformations
from pyspark.ml.feature import StringIndexer
indexer = StringIndexer()\
  .setInputCol("day_of_week")\
  .setOutputCol("day_of_week_index")


# COMMAND ----------
# Using OneHot encoder to fix the numbering scheme of days of the weeks
from pyspark.ml.feature import OneHotEncoder
encoder = OneHotEncoder()\
  .setInputCol("day_of_week_index")\
  .setOutputCol("day_of_week_encoded")


# COMMAND ----------
# The features are encoded in a Vector
from pyspark.ml.feature import VectorAssembler

vectorAssembler = VectorAssembler()\
  .setInputCols(["UnitPrice", "Quantity", "day_of_week_encoded"])\
  .setOutputCol("features")


# COMMAND ----------
# Build a pipeline to represent every stage of learning
from pyspark.ml import Pipeline

transformationPipeline = Pipeline()\
  .setStages([indexer, encoder, vectorAssembler])


# COMMAND ----------
# Fit the training data
fittedPipeline = transformationPipeline.fit(trainDataFrame)


# COMMAND ----------
# Transform all of our data
transformedTraining = fittedPipeline.transform(trainDataFrame)


# COMMAND ----------
# Training the model
from pyspark.ml.clustering import KMeans
kmeans = KMeans()\
  .setK(20)\
  .setSeed(1L)


# COMMAND ----------

kmModel = kmeans.fit(transformedTraining)


# COMMAND ----------

transformedTest = fittedPipeline.transform(testDataFrame)


# --------------------- Low Level Apis ---------------------

# COMMAND ----------
# One thing Resilient Distributed Datasets (RDDs) can do is to parallelize raw 
# data that you have stored in memory on the driver machine
from pyspark.sql import Row

spark.sparkContext.parallelize([Row(1), Row(2), Row(3)]).toDF()


# COMMAND ----------

