import sys
import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# conf = pyspark.SparkConf().set("spark.jars.packages","org.mongodb.spark:mongo-spark-connector_2.12:3.0.1").setAppName("Mongo Extractor")

my_spark = SparkSession \
    .builder \
    .appName("Mongo Extractor") \
    .config("spark.mongodb.input.uri", "mongodb+srv://admin:<password>@cluster0.xxxx.mongodb.net/sample_airbnb.listingsAndReviews") \
    .config("spark.mongodb.output.uri", "mongodb+srv://admin:<password>@cluster0.xxxx.mongodb.net/sample_airbnb.listingsAndReviews") \
    .config("spark.jars.packages","org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

sc = my_spark.sparkContext

sqlC = SQLContext(sc)


listingsAndReviews = sqlC.read.format("mongo").load()

listingsAndReviews.createOrReplaceTempView("tbl_listingsAndReviews")

listingsAndReviews = sqlC.sql("""Select * from tbl_listingsAndReviews""")

listingsAndReviews.show()



