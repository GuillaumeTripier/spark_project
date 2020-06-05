import unittest
import sys
from esgi.exo.FootballApp.FootballApp import FootballApp
from datetime import datetime

class FootballAppTest(unittest.TestCase):
	#def __init__(self, argv):

	def test_change_penality_null_by_0_return_0_when_NA(self):
		result = FootballApp.change_penality_null_by_0("NA")
		
		self.assertEqual(0, result, "Error")

	def test_change_penality_null_by_0_return_14_when_14(self):
		result = FootballApp.change_penality_null_by_0("14")
		
		self.assertEqual(14, result, "Error")

	def test_filter_after_date(self):
		footballApp = FootballApp(sys.argv)
		rdd_given = footballApp.spark.sparkContext.parallelize([
			("Lundi", "2020-06-25"),
			("Mardi", "2020-06-30")
		])
		df_given = footballApp.spark.createDataFrame(rdd_given, ['day', 'date'])
		rdd_expected = footballApp.spark.sparkContext.parallelize([
			("Mardi", "2020-06-30")
		])
		df_expected = footballApp.spark.createDataFrame(rdd_expected, ['day', 'date'])

		df_result = FootballApp.filter_after_date(datetime(2020, 6, 28), df_given)
		
		self.assertListEqual(df_expected.collect(), df_result.collect())

	def test_filter_after_date_when_date_is_newer_than_dates_in_data(self):
		footballApp = FootballApp(sys.argv)
		rdd_given = footballApp.spark.sparkContext.parallelize([
			("Lundi", "2020-06-25"),
			("Mardi", "2020-06-30")
		])
		df_given = footballApp.spark.createDataFrame(rdd_given, ['day', 'date'])

		df_result = FootballApp.filter_after_date(datetime(2020, 7, 28), df_given)
		
		self.assertListEqual([], df_result.collect())

if __name__ == '__main__':
	unittest.main()