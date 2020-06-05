import sys
from datetime import datetime
from pyspark.sql import SparkSession, Window, SQLContext
from pyspark import SparkConf, StorageLevel
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, DateType, StructType, StructField, IntegerType, BooleanType

class FootballApp():

	@staticmethod
	def change_penality_null_by_0(penaltyValue):
		if penaltyValue == "NA" :
			return 0 
		return int(penaltyValue)

	@staticmethod
	def filter_after_date(targetDate, dfToFilter):
		return dfToFilter.filter(dfToFilter.date >= targetDate)

	@staticmethod
	def is_match_played_at_home(match, adversaire):
		mathcSplited = match.split(" - ")
		if len(mathcSplited) == 2 and mathcSplited[1] == adversaire :
			return True 
		return False

	@staticmethod
	def is_match_played_at_world_cup(competition):
		if competition.startswith("Coupe du monde") :
			return True 
		return False

	def __init__(self, argv):
		self.spark = SparkSession.builder.appName("my-spark-app").config("spark.ui.port","5050").getOrCreate()
		self.change_penality_null_by_0_udf = F.udf(self.change_penality_null_by_0, IntegerType())
		self.is_match_played_at_home_udf = F.udf(self.is_match_played_at_home, BooleanType())
		self.is_match_played_at_world_cup_udf = F.udf(self.is_match_played_at_world_cup, BooleanType())
	
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
			StructField("no", StringType(), True)
		])
		dfNotFiltered = SQLContext(self.spark).read.csv(csvFilePath, header=True,schema=schema)
		return dfNotFiltered.filter(dfNotFiltered.no != "None")

	def step_one_clean_data(self, dfMatches):
		dfRenamed = dfMatches.withColumnRenamed("X4", "match").withColumnRenamed("X6", "competition")
		dfSelected = dfRenamed.select("match", "competition", "adversaire", "score_france", "score_adversaire", "penalty_france", "penalty_adversaire", "date", "year")
		dfMatchWithNewCols = (dfSelected
			.withColumn('penalty_france', self.change_penality_null_by_0_udf(dfSelected.penalty_france))
			.withColumn('penalty_adversaire', self.change_penality_null_by_0_udf(dfSelected.penalty_adversaire))
			.withColumn('is_at_home', self.is_match_played_at_home_udf(dfSelected.match, dfSelected.adversaire))
			.withColumn('is_at_world_cup', self.is_match_played_at_world_cup_udf(dfSelected.competition))
		)
		return self.filter_after_date(datetime(1980, 3, 1), dfMatchWithNewCols)

	def step_two_generate_stats(self, dfMatches):
		windowByAdversaire = Window.partitionBy("adversaire")

		avgScoreFrance = F.avg("score_france").over(windowByAdversaire)
		avgScoreAdversaire = F.avg("score_adversaire").over(windowByAdversaire)
		totalMatchCount = F.count("adversaire").over(windowByAdversaire)
		rateMatchPlayedAtHomeCount = F.sum(F.col("is_at_home").cast("long")).over(windowByAdversaire) / F.count("adversaire").over(windowByAdversaire) * 100
		totalMatchWorldCup = F.sum(F.col("is_at_world_cup").cast("long")).over(windowByAdversaire)
		maxPenalityFrance = F.max("penalty_france").over(windowByAdversaire)
		maxPenalityAdversaire = F.max("penalty_adversaire").over(windowByAdversaire)
		
		dfMatchCleaned = (dfMatches
			.drop("competition")
			.drop("match")
			.drop("date")
			.drop("year")
		)
		dfStats = (dfMatchCleaned
			.withColumn("avg_score_france", avgScoreFrance).drop("score_france")
			.withColumn("avg_score_adversaire", avgScoreAdversaire).drop("score_adversaire")
			.withColumn("match_count", totalMatchCount)
			.withColumn("rate_at_home", rateMatchPlayedAtHomeCount).drop("is_at_home")
			.withColumn("at_world_cup_count", totalMatchWorldCup).drop("is_at_world_cup")
			.withColumn("max_penality_france", maxPenalityFrance).drop("penalty_france")
			.withColumn("max_penalty_adversaire", maxPenalityAdversaire).drop("penalty_adversaire")
		)
		dfStats.dropDuplicates().write.mode('overwrite').parquet("output/stats.parquet")

	def step_three_join(self, dfMatchesFiltered):
		dfStats = SQLContext(self.spark).read.parquet('output/stats.parquet')
		dfJoined = dfMatchesFiltered.join(dfStats, "adversaire")
		dfJoined.write.partitionBy("year", "date").mode('overwrite').parquet("output/result.parquet")

	def main(self):
		dfMatches = self.load_dataFrame_from_csv("resource/df_matches.csv")
		dfMatchesFiltered = self.step_one_clean_data(dfMatches)
		dfMatchesFiltered.cache()

		self.step_two_generate_stats(dfMatchesFiltered)
		self.step_three_join(dfMatchesFiltered)
		dfMatchesFiltered.unpersist()

if __name__ == '__main__':
	footballApp = FootballApp(sys.argv)
	footballApp.main()