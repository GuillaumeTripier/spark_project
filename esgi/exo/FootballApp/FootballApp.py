import sys
from datetime import datetime
from pyspark.sql import SparkSession, Window, SQLContext
from pyspark import SparkConf
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, DateType, StructType, StructField, IntegerType

class FootballApp():

	@staticmethod
	def change_penality_null_by_0(penaltyValue):
		penaltyValue = str(penaltyValue)
		if penaltyValue == "NA" :
			return 0 
		return int(penaltyValue)

	def __init__(self, argv):
		self.spark = SparkSession.builder.appName("my-spark-app").config("spark.ui.port","5050").getOrCreate()
		self.change_penality_null_by_0_udf = F.udf(self.change_penality_null_by_0, IntegerType())
	
	def load_dataFrame_from_csv(self, csvFilePath):
		schema = StructType([
			StructField("X2", StringType(), True),
			StructField("X4", StringType(), True),
			StructField("X5", StringType(), True),
			StructField("X6", StringType(), True),
			StructField("adversaire", StringType(), True),
			StructField("score_france", IntegerType(), True),
			StructField("score_adversaire", IntegerType(), True),
			StructField("penalty_france", StringType(), True),
			StructField("penalty_adversaire", StringType(), True),
			StructField("date", DateType(), True),
			StructField("year", IntegerType(), True),
			StructField("outcome", StringType(), True),
			StructField("no", StringType(), True),
		])
		dfNotFiltered = SQLContext(self.spark).read.csv(csvFilePath, header=True,schema=schema)
		return dfNotFiltered.filter(dfNotFiltered.no != "None")

	@staticmethod
	def filter_after_date(targetDate, dfToFilter):
		return dfToFilter.filter(dfToFilter.date >= targetDate)


	def main(self):
		df_matches = self.load_dataFrame_from_csv("res/df_matches.csv")
		df_matches = df_matches.withColumnRenamed("X4", "match")
		df_matches = df_matches.withColumnRenamed("X6", "competition")
		df_matches = df_matches.select("match", "competition", "adversaire", "score_france", "score_adversaire", "penalty_france", "penalty_adversaire", "date")
		df_matches = df_matches.withColumn('penalty_france', self.change_penality_null_by_0_udf(df_matches.penalty_france))
		df_matches = df_matches.withColumn('penalty_adversaire', self.change_penality_null_by_0_udf(df_matches.penalty_adversaire))
		df_matches = self.filter_after_date(datetime(1980, 3, 1), df_matches)

		df_matches.printSchema()
		df_matches.show(100)#5)

if __name__ == '__main__':
	footballApp = FootballApp(sys.argv)
	footballApp.main()