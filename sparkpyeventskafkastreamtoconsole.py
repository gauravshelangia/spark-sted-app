from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, base64, split, concat, lit
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

# TO-DO: using the spark application object, read a streaming dataframe from the Kafka topic stedi-events as the source
# Be sure to specify the option that reads all the events from the topic including those that were published before you started the spark stream
# First Define the customerRisk Schema
customerRiskSchema = StructType([
    StructField("customer", StringType()),
    StructField("score", StringType()),
    StructField("riskDate", DateType())
])

sparkApp = SparkSession.builder\
    .appName("stedi-risk-application")\
    .option("master", "local[*]")\
    .getOrCreate()
sparkApp.sparkContext.setLogLevel('WARN')

# Risk data is not appearing under stedi-events topic
# kafkaRiskStream = sparkApp.readStream\
#     .format("kafka")\
#     .option("kafka.bootstrap.servers", "localhost:9092")\
#     .option("subscribe", "stedi-events")\
#     .option("startingOffsets", "earliest")\
#     .load()

riskStreamingFile = sparkApp.readStream.text('stedi-application')

# TO-DO: cast the value column in the streaming dataframe as a STRING 
# TO-DO: parse the JSON from the single column "value" with a json object in it, like this:
# +------------+
# | value      |
# +------------+
# |{"custom"...|
# +------------+
# commenting it out ..no data from kafka
# kafkaRiskStreamDF = kafkaRiskStream.selectExpr("CAST(value as STRING)")
logRiskStreamDF = riskStreamingFile.filter(riskStreamingFile.value.contains('INFO: Risk for customer: '))

logRiskStreamPartialDF = logRiskStreamDF.select(split(logRiskStreamDF.value, "\{").getItem(1).alias("riskPayload"))

kafkaRiskStreamDF = logRiskStreamPartialDF.select(concat(lit("{"),logRiskStreamPartialDF.riskPayload).alias("riskJson"))
kafkaRiskStreamDF.printSchema()
# and create separated fields like this:
# +------------+-----+-----------+
# |    customer|score| riskDate  |
# +------------+-----+-----------+
# |"sam@tes"...| -1.4| 2020-09...|
# +------------+-----+-----------+
#
# storing them in a temporary view called CustomerRisk
kafkaRiskStreamDF.withColumn("riskJson", from_json("riskJson", customerRiskSchema))\
    .select(col('riskJson.*'))\
    .createOrReplaceTempView("CustomerRisk")
kafkaRiskStreamDF.printSchema()
# TO-DO: execute a sql statement against a temporary view, selecting the customer and the score from the temporary view, creating a dataframe called customerRiskStreamingDF
customerRiskStreamingDF = sparkApp.sql("select customer, score from CustomerRisk")
# TO-DO: sink the customerRiskStreamingDF dataframe to the console in append mode
customerRiskStreamingDF.printSchema() 
# It should output like this:
#
# +--------------------+-----
# |customer           |score|
# +--------------------+-----+
# |Spencer.Davis@tes...| 8.0|
# +--------------------+-----
customerRiskStreamingDF.writeStream.outputMode("append").format("console").start().awaitTermination()
# Run the python script by running the command from the terminal:
# /home/workspace/submit-event-kafkastreaming.sh
# Verify the data looks correct 