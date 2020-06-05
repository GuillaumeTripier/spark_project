import unittest
import sys
from esgi.exo.FootballApp.FootballApp import FootballApp
from datetime import datetime
from pyspark.sql.types import StringType, DateType, StructType, StructField, IntegerType, BooleanType

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

	def test_is_match_played_at_home_when_it_is_true(self):
		match = "France - Allemagne"
		adversaire = "Allemagne"

		result = FootballApp.is_match_played_at_home(match, adversaire)
		
		self.assertEqual(True, result)

	def test_is_match_played_at_home_when_it_is_false(self):
		match = "Allemagne - France"
		adversaire = "Allemagne"

		result = FootballApp.is_match_played_at_home(match, adversaire)
		
		self.assertEqual(False, result)

	def test_is_match_played_at_home_when_match_text_has_not_the_good_format(self):
		match = "France-Allemagne"
		adversaire = "Allemagne"

		result = FootballApp.is_match_played_at_home(match, adversaire)
		
		self.assertEqual(False, result)

	def test_is_match_played_at_world_cup_when_it_is_true(self):
		competition = "Coupe du monde 2018"

		result = FootballApp.is_match_played_at_world_cup(competition)
		
		self.assertEqual(True, result)

	def test_is_match_played_at_world_cup_when_it_is_false(self):
		competition = "Qualif EURO 2020"

		result = FootballApp.is_match_played_at_world_cup(competition)
		
		self.assertEqual(False, result)

	def test_change_penality_null_by_0_udf(self):
		footballApp = FootballApp(sys.argv)
		rdd_given = footballApp.spark.sparkContext.parallelize([
			("Lundi", "NA"),
			("Mardi", "2")
		])
		schema_given = StructType([
			StructField("day", StringType(), True),
			StructField("penalty", StringType(), True),
		])
		df_given = footballApp.spark.createDataFrame(rdd_given, schema=schema_given)
		rdd_expected = footballApp.spark.sparkContext.parallelize([
			("Lundi", "NA", 0),
			("Mardi", "2", 2)
		])
		schema_expected = StructType([
			StructField("day", StringType(), True),
			StructField("penalty", StringType(), True),
			StructField("penalty_result", IntegerType(), True),
		])
		df_expected = footballApp.spark.createDataFrame(rdd_expected, schema=schema_expected)

		df_result = df_given.withColumn('penalty_result', footballApp.change_penality_null_by_0_udf(df_given.penalty))

		self.assertListEqual(df_expected.collect(), df_result.collect())

	def test_is_match_played_at_home_udf(self):
		footballApp = FootballApp(sys.argv)
		rdd_given = footballApp.spark.sparkContext.parallelize([
			("France - Allemagne", "Allemagne"),
			("Allemagne- France", "Allemagne")
		])
		schema_given = StructType([
			StructField("match", StringType(), True),
			StructField("adversaire", StringType(), True),
		])
		df_given = footballApp.spark.createDataFrame(rdd_given, schema=schema_given)
		rdd_expected = footballApp.spark.sparkContext.parallelize([
			("France - Allemagne", "Allemagne", True),
			("Allemagne- France", "Allemagne", False)
		])
		schema_expected = StructType([
			StructField("match", StringType(), True),
			StructField("adversaire", StringType(), True),
			StructField("is_at_home", BooleanType(), True),
		])
		df_expected = footballApp.spark.createDataFrame(rdd_expected, schema=schema_expected)

		df_result = df_given.withColumn('is_at_home', footballApp.is_match_played_at_home_udf(df_given.match, df_given.adversaire))

		self.assertListEqual(df_expected.collect(), df_result.collect())

	def test_is_match_played_at_world_cup_udf(self):
		footballApp = FootballApp(sys.argv)
		rdd_given = footballApp.spark.sparkContext.parallelize([
			("France - Allemagne", "Coupe du monde 2014"),
			("Allemagne- France", "Qualif EURO 2012")
		])
		schema_given = StructType([
			StructField("match", StringType(), True),
			StructField("competition", StringType(), True),
		])
		df_given = footballApp.spark.createDataFrame(rdd_given, schema=schema_given)
		rdd_expected = footballApp.spark.sparkContext.parallelize([
			("France - Allemagne", "Coupe du monde 2014", True),
			("Allemagne- France", "Qualif EURO 2012", False)
		])
		schema_expected = StructType([
			StructField("match", StringType(), True),
			StructField("competition", StringType(), True),
			StructField("is_at_world_cup", BooleanType(), True),
		])
		df_expected = footballApp.spark.createDataFrame(rdd_expected, schema=schema_expected)

		df_result = df_given.withColumn('is_at_world_cup', footballApp.is_match_played_at_world_cup_udf(df_given.competition))

		self.assertListEqual(df_expected.collect(), df_result.collect())	

if __name__ == '__main__':
	unittest.main()