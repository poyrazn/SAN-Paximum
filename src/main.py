from pyspark.sql.context import SQLContext

from pyspark.sql import SparkSession
from pyspark import SparkFiles
from pyspark.sql import Catalog
from pyspark.sql import DataFrameWriter
from pyspark.sql.types import DecimalType

path = "../data/PaximumSearchData.json"

spark = SparkSession.builder.master("local").appName("Data Visualization").config("spark.executor.memory", "1gb").getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)
sc.addFile(path)
df = spark.read.json(SparkFiles.get("PaximumSearchData.json")).drop('_corrupt_record').dropna(how='all')


df = df \
	.withColumn('adults', df.Rooms.Adults) \
	.withColumn('childrenages', df.Rooms.ChildrenAges) \
	.withColumn('hotelname', df.Destination.Hotel.Name) \
	.withColumn('city', df.Destination.Destination.Name) \
	.withColumn('country', df.Destination.Destination.CountryName) \
	.withColumn('searchtype', df.Destination.Type) \
	.withColumn('requestredproductcount', df.Destination.RequestedProductCount.cast("integer")) \
	.withColumnRenamed('Date', 'searchdate') \
	.withColumnRenamed('CheckIn', 'checkindate') \
	.withColumnRenamed('CheckOut', 'checkoutdate') \
	.withColumnRenamed('Period', 'period') \
	.withColumnRenamed('ResponseTime', 'responsetime') \
	.withColumnRenamed('Nationality', 'nationality') \
	.withColumnRenamed('UserId', 'userid') \
	.withColumnRenamed('Currency', 'currency') \
	.withColumnRenamed('UserName', 'username') \
	.withColumnRenamed('HotelCount', 'hotelcount') \
	.withColumn('accountroot', df.Accounts.Name[0]) \
	.drop('Destinations', 'Account', 'Accounts', 'Rooms')

#
print(df.show(5))

#
# sqlContext.registerDataFrameAsTable(df, 'Data')
# print(sqlContext.tableNames())
# print(sqlContext.table('Data').show(5))


# df.write.format('json').saveAsTable('Data')
# print(spark.catalog.listColumns('Data'))
# df2 = sqlContext.table("Data")
# print(df2.show(5))


# from pydruid.db import connect
# conn = connect(host='localhost', port=8082, path='/druid/v2/sql/', scheme='http')
# cursor = conn.cursor()
# query = "SELECT * FROM PaximumTest"
# cursor.execute(query)
# data = cursor.fetchall()


