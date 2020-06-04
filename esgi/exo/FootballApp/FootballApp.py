import sys
from pyspark.sql import SparkSession, Window
from pyspark import SparkConf
import pyspark.sql.functions as F
from pyspark.sql.types import StringType

class FootballApp():
	def __init__(self, argv):
		self.spark = SparkSession.builder.appName("my-spark-app").config("spark.ui.port","5050").getOrCreate()
	
	def main(self):
		input_data = [("paris", "75000", 2187526),("marseille", "13000", 863310)]
		rdd_input = self.spark.sparkContext.parallelize(input_data)
		df_input = self.spark.createDataFrame(rdd_input, ['city', 'zip_Code', 'population'])
		df_input.printSchema()
		df_input.show()

if __name__ == '__main__':
	footballApp = FootballApp(sys.argv)
	footballApp.main()