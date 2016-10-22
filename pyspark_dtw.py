
# Tested using Spark 1.6.2 and Anaconda 2.7

from pyspark import SparkContext, SparkConf
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import fastdtw

conf = SparkConf().setAppName("spark_dtw").setMaster("yarn-client")
sc   = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

# Import Data
data   = sc.textFile("hdfs://sandbox.hortonworks.com:/tmp/household_power_consumption_uci_edu.txt")
header = data.first().split(';')
data   = data.map(lambda x: x.split(';')).filter(lambda x: x!=header)

df = data.toDF(header)

# Use schema
df1 = df.withColumn('Sub_metering_1',df.Sub_metering_1.cast('float')) \
    .withColumn('Sub_metering_2',df.Sub_metering_2.cast('float')) \
    .withColumn('Sub_metering_3',df.Sub_metering_3.cast('float')) \
    .withColumn('Voltage',df.Voltage.cast('float'))

df1.show(5)

df2 = df1.groupby("Date").agg(F.collect_list("Sub_metering_1").alias('meter1'),F.collect_list("Sub_metering_2").alias('meter2'),F.collect_list("Sub_metering_3").alias('meter3'))

df2.show(5)

def get_udf_distance(array1, array2):
    distance, path = fastdtw(array1, array2, dist=euclidean)
    return distance

udf_dtw = udf(get_udf_distance , FloatType())
df3 = df2.select('Date', udf_dtw(df2.meter1, df2.meter2).alias('dtw_distance (meter1-meter2)'))

df3.show()
