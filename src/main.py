from pyspark.sql import SparkSession
from pyspark import SparkFiles
import os


def init(datapath):
	spark = SparkSession.builder.master("local").appName("Data Visualization").config("spark.executor.memory", "1gb").getOrCreate()
	sc = spark.sparkContext
	sc.addFile(datapath)
	df = spark.read.json(SparkFiles.get("PaximumSearchData.json")).drop('_corrupt_record').dropna(how='all')
	return df


def applyChanges(df):
	df = df \
		.withColumn('adults', df.Rooms.Adults) \
		.withColumn('childrenages', df.Rooms.ChildrenAges) \
		.withColumn('hotelname', df.Destination.Hotel.Name) \
		.withColumn('city', df.Destination.Destination.Name) \
		.withColumn('country', df.Destination.Destination.CountryName) \
		.withColumn('searchtype', df.Destination.Type) \
		.withColumn('requestredproductcount', df.Destination.RequestedProductCount.cast("integer")) \
		.withColumn('checkindate', df.CheckIn['$date']) \
		.withColumn('checkoutdate', df.CheckOut['$date']) \
		.withColumn('searchdate', df.Date['$date']) \
		.withColumn('responsetime', df.ResponseTime['$numberLong']) \
		.withColumn('accountroot', df.Accounts.Name[0]) \
		.drop('Destinations', 'Account', 'Accounts', 'Rooms', '_id', 'Destination', 'Date', 'CheckIn', 'CheckOut')
	return df


def ingestion(taskpath, filepath, url):
	command = f"{taskpath} --file {filepath} --url {url}"
	os.system(command)


if __name__ == '__main__':
	path = "../data/PaximumSearchData.json"

	df = init(path)
	df = applyChanges(df)
	df.write.json('../data/paximum')

	indexTaskPath = "/Users/nehir/apache-druid-0.17.0/bin/post-index-task"
	filepath = "/Users/nehir/Downloads/SanData/data/paximum-index.json"
	url = "http://localhost:8081"

	ingestion(indexTaskPath, filepath, url)
