from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr, concat, lit, struct
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

# TO-DO: create a StructType for the Kafka redis-server topic which has all changes made to Redis - before Spark 3.0.0, schema inference is not automatic
kafkaRedisSchema = StructType([
    StructField("key", StringType()),
    StructField("existType", StringType()),
    StructField("ch", BooleanType()),
    StructField("incr", BooleanType()),
    StructField("zSetEntries", ArrayType(\
        StructType([
            StructField("element", StringType()),\
            StructField("score", StringType())\
        ])
    ))
    ]
)
# TO-DO: create a StructType for the Customer JSON that comes from Redis- before Spark 3.0.0, schema inference is not automatic
customerSchema = StructType([
    StructField("customerName", StringType()),
    StructField("email", StringType()),
    StructField("phone", StringType()),
    StructField("birthDay", DateType())
    ]
)

# TO-DO: create a StructType for the Kafka stedi-events topic which has the Customer Risk JSON that comes from Redis- before Spark 3.0.0, schema inference is not automatic
customerRiskSchema = StructType([
    StructField("customer", StringType()),
    StructField("score", StringType()),
    StructField("riskDate", DateType())
])

#TO-DO: create a spark application object
sparkApp = SparkSession.builder\
    .appName("sted-application-1")\
    .getOrCreate()

#TO-DO: set the spark log level to WARN
sparkApp.sparkContext.setLogLevel('WARN')

# TO-DO: using the spark application object, read a streaming dataframe from the Kafka topic redis-server as the source
# Be sure to specify the option that reads all the events from the topic including those that were published before you started the spark stream
kafkaRedisStream = sparkApp.readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe","redis-server")\
    .option("startingOffsets", "earliest")\
    .load()

# TO-DO: cast the value column in the streaming dataframe as a STRING 
kafkaRedisStreamDF = kafkaRedisStream.selectExpr("CAST(value as STRING)")

# TO-DO:; parse the single column "value" with a json object in it, like this:
# +------------+
# | value      |
# +------------+
# |{"key":"Q3..|
# +------------+
#
# with this JSON format: {"key":"Q3VzdG9tZXI=",
# "existType":"NONE",
# "Ch":false,
# "Incr":false,
# "zSetEntries":[{
# "element":"eyJjdXN0b21lck5hbWUiOiJTYW0gVGVzdCIsImVtYWlsIjoic2FtLnRlc3RAdGVzdC5jb20iLCJwaG9uZSI6IjgwMTU1NTEyMTIiLCJiaXJ0aERheSI6IjIwMDEtMDEtMDMifQ==",
# "Score":0.0
# }],
# "zsetEntries":[{
# "element":"eyJjdXN0b21lck5hbWUiOiJTYW0gVGVzdCIsImVtYWlsIjoic2FtLnRlc3RAdGVzdC5jb20iLCJwaG9uZSI6IjgwMTU1NTEyMTIiLCJiaXJ0aERheSI6IjIwMDEtMDEtMDMifQ==",
# "score":0.0
# }]
# }
# 
# (Note: The Redis Source for Kafka has redundant fields zSetEntries and zsetentries, only one should be parsed)
#
# and create separated fields like this:
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
# |         key|value|expiredType|expiredValue|existType|   ch| incr|      zSetEntries|
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
# |U29ydGVkU2V0| null|       null|        null|     NONE|false|false|[[dGVzdDI=, 0.0]]|
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
#
# storing them in a temporary view called RedisSortedSet
kafkaRedisStreamDF\
    .withColumn("value", from_json("value", kafkaRedisSchema))\
    .select(col('value.*'))\
    .createOrReplaceTempView("RedisSortedSet")
# TO-DO: execute a sql statement against a temporary view, which statement takes the element field from the 0th element in the array of structs and create a column called encodedCustomer
# the reason we do it this way is that the syntax available select against a view is different than a dataframe, and it makes it easy to select the nth element of an array in a sql column
encodedCustomerStreamDF = sparkApp.sql("select key, zSetEntries[0].element as encodedCustomer from RedisSortedSet")

# TO-DO: take the encodedCustomer column which is base64 encoded at first like this:
# +--------------------+
# |            customer|
# +--------------------+
# |[7B 22 73 74 61 7...|
# +--------------------+

# and convert it to clear json like this:
# +--------------------+
# |            customer|
# +--------------------+
# |{"customerName":"...|
#+--------------------+
#
# with this JSON format: {"customerName":"Sam Test","email":"sam.test@test.com","phone":"8015551212","birthDay":"2001-01-03"}
decodedCustomerStreamDF = encodedCustomerStreamDF.withColumn("customer", unbase64(encodedCustomerStreamDF.encodedCustomer).cast("string"))

# TO-DO: parse the JSON in the Customer record and store in a temporary view called CustomerRecords
decodedCustomerStreamDF\
    .withColumn("customer", from_json("customer", customerSchema))\
    .select(col('customer.*'))\
    .createOrReplaceTempView("CustomerRecords")

# TO-DO: JSON parsing will set non-existent fields to null, so let's select just the fields we want, where they are not null as a new dataframe called emailAndBirthDayStreamingDF
emailAndBirthDayStreamingDF = sparkApp.sql("select birthDay, email from CustomerRecords where birthDay is not null and email is not null")

# TO-DO: Split the birth year as a separate field from the birthday
# TO-DO: Select only the birth year and email fields as a new streaming data frame called emailAndBirthYearStreamingDF
emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF.select('email', split(emailAndBirthDayStreamingDF.birthDay,"-").getItem(0).alias("birthYear"))

emailAndBirthYearStreamingDF.writeStream.outputMode("append").format("console").start()
# TO-DO: using the spark application object, read a streaming dataframe from the Kafka topic stedi-events as the source
# Be sure to specify the option that reads all the events from the topic including those that were published before you started the spark stream
print("Start reading file")                                   
# TO-DO: cast the value column in the streaming dataframe as a STRING 

riskStreamingFile = sparkApp.readStream.text('stedi-application')

# TO-DO: parse the JSON from the single column "value" with a json object in it, like this:
# +------------+
# | value      |
# +------------+
# |{"custom"...|
# +------------+
#
# and create separated fields like this:
# +------------+-----+-----------+
# |    customer|score| riskDate  |
# +------------+-----+-----------+
# |"sam@tes"...| -1.4| 2020-09...|
# +------------+-----+-----------+
#
# storing them in a temporary view called CustomerRisk
logRiskStreamDF = riskStreamingFile.filter(riskStreamingFile.value.contains('INFO: Risk for customer: '))

logRiskStreamPartialDF = logRiskStreamDF.select(split(logRiskStreamDF.value, "\{").getItem(1).alias("riskPayload"))

kafkaRiskStreamDF = logRiskStreamPartialDF.select(concat(lit("{"),logRiskStreamPartialDF.riskPayload).alias("riskJson"))

kafkaRiskStreamDF.withColumn("riskJson", from_json("riskJson", customerRiskSchema))\
    .select(col('riskJson.*'))\
    .createOrReplaceTempView("CustomerRisk")
# TO-DO: execute a sql statement against a temporary view, selecting the customer and the score from the temporary view, creating a dataframe called customerRiskStreamingDF
customerRiskStreamingDF = sparkApp.sql("select customer, score from CustomerRisk")

customerRiskStreamingDF.writeStream.outputMode("append").format("console").start()

# TO-DO: join the streaming dataframes on the email address to get the risk score and the birth year in the same dataframe

# TO-DO: sink the joined dataframes to a new kafka topic to send the data to the STEDI graph application 
# +--------------------+-----+--------------------+---------+
# |            customer|score|               email|birthYear|
# +--------------------+-----+--------------------+---------+
# |Santosh.Phillips@...| -0.5|Santosh.Phillips@...|     1960|
# |Sean.Howard@test.com| -3.0|Sean.Howard@test.com|     1958|
# |Suresh.Clark@test...| -5.0|Suresh.Clark@test...|     1956|
# |  Lyn.Davis@test.com| -4.0|  Lyn.Davis@test.com|     1955|
# |Sarah.Lincoln@tes...| -2.0|Sarah.Lincoln@tes...|     1959|
# |Sarah.Clark@test.com| -4.0|Sarah.Clark@test.com|     1957|
# +--------------------+-----+--------------------+---------+
#
# In this JSON Format {"customer":"Santosh.Fibonnaci@test.com","score":"28.5","email":"Santosh.Fibonnaci@test.com","birthYear":"1963"}
joinedCustomerDF = customerRiskStreamingDF.withColumn("email", col("customer")).join(emailAndBirthYearStreamingDF, on="email").select("customerrisk.customer","customerrisk.score", "email", "birthYear")

# joinedCustomerDF = customerRiskStreamingDF.join(emailAndBirthYearStreamingDF, emailAndBirthYearStreamingDF.email== customerRiskStreamingDF.customer)

joinedCustomerDF.printSchema()
# joinedCustomerDF.show(n=3)
# joinedCustomerDF.writeStream.outputMode("append").format("console")\
#       .option("truncate", "false")\
#       .start()\
#       .awaitTermination()
# joinedCustomerDF.withColumn("value", to_json())

kafkaDataFrame = joinedCustomerDF.select(col("email"), to_json(struct([joinedCustomerDF[x] for x in joinedCustomerDF.columns]))).toDF("key","value")

# (joinedCustomerDF.select(to_json(struct([joinedCustomerDF[x] for x in joinedCustomerDF.columns])).alias("value"))
#     .writeStream
#     .format("kafka")
#     .option("kafka.bootstrap.servers", "localhost:9092")
#     .option("topic", "topic-risk-score")
#     .option("checkpointLocation", "/tmp/kafka/checkpoint")
#     .start())
kafkaDataFrame.printSchema()

# kafkaDataFrame.writeStream\
#       .outputMode("append").format("console")\
#       .option("truncate", "false")\
#       .start().awaitTermination()

kafkaDataFrame.selectExpr("key", "value").writeStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers","localhost:9092")\
    .option("topic","topic-risk-score")\
    .option("checkpointLocation", "spark/kafka")\
    .start().awaitTermination()