# read dataframe used for this analysis
df = spark.read.format("csv")\
  .option("header", "true")\
  .option("inferSchema", "true")\
  .load("/data/retail-data/by-day/2010-12-01.csv")
df.printSchema()
df.createOrReplaceTempView("dfTable")

# ----------------------------- Converting to Spark types ----------------------------- 

# COMMAND ----------
# lit function converts a tipe in another language to its corresponding Spark representation 
from pyspark.sql.functions import lit
df.select(lit(5), lit("five"), lit(5.0))


# ----------------------------- Working with Booleans ----------------------------- 
# COMMAND ----------
# working with conditionals
from pyspark.sql.functions import col
df.where(col("InvoiceNo") != 536365)\
  .select("InvoiceNo", "Description")\
  .show(5, False)


# COMMAND ----------
# chaining conditional together
from pyspark.sql.functions import instr
priceFilter = col("UnitPrice") > 600
descripFilter = instr(df.Description, "POSTAGE") >= 1
df.where(df.StockCode.isin("DOT")).where(priceFilter | descripFilter).show()


# COMMAND ----------
# To filter a DataFrame, you can also specify a Boolean column
from pyspark.sql.functions import instr
DOTCodeFilter = col("StockCode") == "DOT"
priceFilter = col("UnitPrice") > 600
descripFilter = instr(col("Description"), "POSTAGE") >= 1
df.withColumn("isExpensive", DOTCodeFilter & (priceFilter | descripFilter))\
  .where("isExpensive")\
  .select("unitPrice", "isExpensive").show(5)


# COMMAND ----------
# Using Spark SQL interface 
from pyspark.sql.functions import expr
df.withColumn("isExpensive", expr("NOT UnitPrice <= 250"))\
  .where("isExpensive")\
  .select("Description", "UnitPrice").show(5)


# ----------------------------- Working with Numbers ----------------------------- 

# COMMAND ----------
# Introduction of the first numerical functions as well as the pow function
from pyspark.sql.functions import expr, pow
fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
df.select(expr("CustomerId"), fabricatedQuantity.alias("realQuantity")).show(2)


# COMMAND ----------
# We can do the previous thing in sql as well
df.selectExpr(
  "CustomerId",
  "(POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity").show(2)


# COMMAND ----------
# Rounding up with round and rounding down with bround
from pyspark.sql.functions import lit, round, bround

df.select(round(lit("2.5")), bround(lit("2.5"))).show(2)


# COMMAND ----------
# We can calculate the correlation of two columns through the DataFrame statistic methods
from pyspark.sql.functions import corr
df.stat.corr("Quantity", "UnitPrice")
df.select(corr("Quantity", "UnitPrice")).show()


# COMMAND ----------
# Compute Summary statistics
df.describe().show()


# COMMAND ----------
# Aggregation functions
from pyspark.sql.functions import count, mean, stddev_pop, min, max


# COMMAND ----------
# Example of functions in StatFunctions Package available using stat
colName = "UnitPrice"
quantileProbs = [0.5]
relError = 0.05
df.stat.approxQuantile("UnitPrice", quantileProbs, relError) # 2.51


# COMMAND ----------
# see cros-tabulation or frequent item pairs
df.stat.crosstab("StockCode", "Quantity").show()


# COMMAND ----------

df.stat.freqItems(["StockCode", "Quantity"]).show()


# COMMAND ----------
# This generates a unique value for each row, starting with 0
from pyspark.sql.functions import monotonically_increasing_id
df.select(monotonically_increasing_id()).show(2)

# ----------------------------- Working with Strings ----------------------------- 

# COMMAND ----------
# initcap function will capitalize every word in a given string 
# when that word is separated from another by a space.
from pyspark.sql.functions import initcap
df.select(initcap(col("Description"))).show()


# COMMAND ----------
# strings can be casted in uppercase and lowercase as well
from pyspark.sql.functions import lower, upper
df.select(col("Description"),
    lower(col("Description")),
    upper(lower(col("Description")))).show(2)

 
# COMMAND ----------
# Adding or removing spaces around a string
from pyspark.sql.functions import lit, ltrim, rtrim, rpad, lpad, trim
df.select(
    ltrim(lit("    HELLO    ")).alias("ltrim"),
    rtrim(lit("    HELLO    ")).alias("rtrim"),
    trim(lit("    HELLO    ")).alias("trim"),
    lpad(lit("HELLO"), 3, " ").alias("lp"),
    rpad(lit("HELLO"), 10, " ").alias("rp")).show(2)


# COMMAND ----------
# Spark uses Java regular expressions syntax
from pyspark.sql.functions import regexp_replace
regex_string = "BLACK|WHITE|RED|GREEN|BLUE"
df.select(
  regexp_replace(col("Description"), regex_string, "COLOR").alias("color_clean"),
  col("Description")).show(2)


# COMMAND ----------
# replace given characters with other characters
from pyspark.sql.functions import translate
df.select(translate(col("Description"), "LEET", "1337"),col("Description"))\
  .show(2)


# COMMAND ----------
# Pull out the first mentioned color
from pyspark.sql.functions import regexp_extract
extract_str = "(BLACK|WHITE|RED|GREEN|BLUE)"
df.select(
     regexp_extract(col("Description"), extract_str, 1).alias("color_clean"),
     col("Description")).show(2)


# COMMAND ----------
# instr can be used to check the existence of a string
from pyspark.sql.functions import instr
containsBlack = instr(col("Description"), "BLACK") >= 1
containsWhite = instr(col("Description"), "WHITE") >= 1
df.withColumn("hasSimpleColor", containsBlack | containsWhite)\
  .where("hasSimpleColor")\
  .select("Description").show(3, False)


# COMMAND ----------
# locate is used to locate a string in column. this value is casted as boolean 
from pyspark.sql.functions import expr, locate
simpleColors = ["black", "white", "red", "green", "blue"]
def color_locator(column, color_string):
  return locate(color_string.upper(), column)\
          .cast("boolean")\
          .alias("is_" + color_string)
selectedColumns = [color_locator(df.Description, c) for c in simpleColors]
selectedColumns.append(expr("*")) # has to a be Column type

df.select(*selectedColumns).where(expr("is_white OR is_red"))\
  .select("Description").show(3, False)


# ----------------------------- Working with Dates and Timestamps ----------------------------- 

# COMMAND ----------
# Get current date and current timestamp
from pyspark.sql.functions import current_date, current_timestamp
dateDF = spark.range(10)\
  .withColumn("today", current_date())\
  .withColumn("now", current_timestamp())
dateDF.createOrReplaceTempView("dateTable")


# COMMAND ----------
# Add five dates from now
from pyspark.sql.functions import date_add, date_sub
dateDF.select(date_sub(col("today"), 5), date_add(col("today"), 5)).show(1)


# COMMAND ----------
# Comparing two dates
from pyspark.sql.functions import datediff, months_between, to_date
dateDF.withColumn("week_ago", date_sub(col("today"), 7))\
  .select(datediff(col("week_ago"), col("today"))).show(1)

dateDF.select(
    to_date(lit("2016-01-01")).alias("start"),
    to_date(lit("2017-05-22")).alias("end"))\
  .select(months_between(col("start"), col("end"))).show(1)


# COMMAND ----------
# to_date function allows you to convert a string to a date
from pyspark.sql.functions import to_date, lit
spark.range(5).withColumn("date", lit("2017-01-01"))\
  .select(to_date(col("date"))).show(1)

# COMMAND ----------
# Spark will not throw an error if cannot parse a date, rather, it will just return null
dateDF.select(to_date(lit("2016-20-12")),to_date(lit("2017-12-11"))).show(1)

# COMMAND ----------
# Fix error of the command above, specifying a new function
from pyspark.sql.functions import to_date
dateFormat = "yyyy-dd-MM"
cleanDateDF = spark.range(1).select(
    to_date(lit("2017-12-11"), dateFormat).alias("date"),
    to_date(lit("2017-20-12"), dateFormat).alias("date2"))
cleanDateDF.createOrReplaceTempView("dateTable2")


# COMMAND ----------
# to_timestamp always requires a format to be specified
from pyspark.sql.functions import to_timestamp
cleanDateDF.select(to_timestamp(col("date"), dateFormat)).show()


# ----------------------------- Working with Nulls in Data ----------------------------- 


# COMMAND ----------
# Coalesce function allows you to select the first non-null valure from a set of columns
from pyspark.sql.functions import coalesce
df.select(coalesce(col("Description"), col("CustomerId"))).show()


# COMMAND ----------
# 'any' drops a row if any of the values are null, all if all the values are null or nan
# we can also appy this to certain sets of columns by passing in an array of columns
df.na.drop("all", subset=["StockCode", "InvoiceNo"])


# COMMAND ----------
# Filling columns wit value all
df.na.fill("all", subset=["StockCode", "InvoiceNo"])


# COMMAND ----------
# we can fill values with a map (dictionary)
fill_cols_vals = {"StockCode": 5, "Description" : "No Value"}
df.na.fill(fill_cols_vals)


# COMMAND ----------
# replace all values in a certain column according to their current value
df.na.replace([""], ["UNKNOWN"], "Description")


# ----------------------------- Working with Complex Types ----------------------------- 

# COMMAND ----------
# we can create a struct by wrapping a set of columns in parenthesis in a query
from pyspark.sql.functions import struct
complexDF = df.select(struct("Description", "InvoiceNo").alias("complex"))
complexDF.createOrReplaceTempView("complexDF")

# COMMAND ----------
# We now have a DataFrame with a column complex
complexDF.select("complex.Description")
complexDF.select(col("complex").getField("Description"))

# COMMAND ----------
# Querying all values in the struct
complexDF.select("complex.*")

# COMMAND ----------
# take every word in the description table and show it
from pyspark.sql.functions import split
df.select(split(col("Description"), " ")).show(2)


# COMMAND ----------
# we can quwery the values of the array using Python-like syntax
df.select(split(col("Description"), " ").alias("array_col"))\
  .selectExpr("array_col[0]").show(2)


# COMMAND ----------
# we can determine the array's length by querying for its size
from pyspark.sql.functions import size
df.select(size(split(col("Description"), " "))).show(2) # shows 5 and 3


# COMMAND ----------
# We can also see whether this array contains a value
from pyspark.sql.functions import array_contains
df.select(array_contains(split(col("Description"), " "), "WHITE")).show(2)


# COMMAND ----------
# The explode function takes a column that consists of arrays and creates one row (with
# the rest of the values duplicated) per value in the array
from pyspark.sql.functions import split, explode

df.withColumn("splitted", split(col("Description"), " "))\
  .withColumn("exploded", explode(col("splitted")))\
  .select("Description", "InvoiceNo", "exploded").show(2)


# COMMAND ----------
# Maps are created by using the map function, and key-value pairs of columns
# You can select them just like you might select from an array
from pyspark.sql.functions import create_map
df.select(create_map(col("Description"), col("InvoiceNo")).alias("complex_map"))\
  .show(2)


# COMMAND ----------
# We can query them by using a proper key. A missing key returns null
df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map"))\
  .selectExpr("complex_map['WHITE METAL LANTERN']").show(2)


# COMMAND ----------
# We can also explode map types, which will turn them into columns
df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map"))\
  .selectExpr("explode(complex_map)").show(2)


# COMMAND ----------
# Spark has some unique support for working with JSON data. You can operate directly
# on strings of JSON in Spark and parse from JSON or extract JSON objects.
jsonDF = spark.range(1).selectExpr("""
  '{"myJSONKey" : {"myJSONValue" : [1, 2, 3]}}' as jsonString""")


# COMMAND ----------
# You can use the get_json_object to inline query a JSON object, be it a dictionary 
# or array. You can use json_tuple if this object has only one level of nesting
from pyspark.sql.functions import get_json_object, json_tuple

jsonDF.select(
    get_json_object(col("jsonString"), "$.myJSONKey.myJSONValue[1]") as "column",
    json_tuple(col("jsonString"), "myJSONKey")).show(2)


# COMMAND ----------
# You can also turn a StructType into a JSON string by using the to_json function
from pyspark.sql.functions import to_json
df.selectExpr("(InvoiceNo, Description) as myStruct")\
  .select(to_json(col("myStruct")))


# COMMAND ----------
# This function also accepts a dictionary (map) of parameters that are the same as the JSON data
# source. You can use the from_json function to parse this (or other JSON data) back in. This
# naturally requires you to specify a schema, and optionally you can specify a map of options as well.
from pyspark.sql.functions import from_json
from pyspark.sql.types import *
parseSchema = StructType((
  StructField("InvoiceNo",StringType(),True),
  StructField("Description",StringType(),True)))
df.selectExpr("(InvoiceNo, Description) as myStruct")\
  .select(to_json(col("myStruct")).alias("newJSON"))\
  .select(from_json(col("newJSON"), parseSchema), col("newJSON")).show(2)


# ----------------------------- User-Defined Functions ----------------------------- 


# COMMAND ----------
# create an UDF, pass that into Spark and then execute code using that UDF
udfExampleDF = spark.range(5).toDF("num")
def power3(double_value):
  return double_value ** 3
power3(2.0)


# COMMAND ----------
# we need to register the function
from pyspark.sql.functions import udf
power3udf = udf(power3)


# COMMAND ----------
# then, we use it in our DataFrame code
from pyspark.sql.functions import col
udfExampleDF.select(power3udf(col("num"))).show(2)


# COMMAND ----------
# We can also use it as a SQL expression
udfExampleDF.selectExpr("power3(num)").show(2)
# registered in Scala


# COMMAND ----------
# is recommended to specify the return type of the UDF function
# If you specify the type that doesn’t align with the actual type returned by the function, Spark will
# not throw an error but will just return null to designate a failure.
# This wont convert them into float, therefore, we see null
from pyspark.sql.types import IntegerType, DoubleType
spark.udf.register("power3py", power3, DoubleType())


# COMMAND ----------

udfExampleDF.selectExpr("power3py(num)").show(2)
# registered via Python


# COMMAND ----------

