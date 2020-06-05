import sys
from datetime import datetime
from pyspark.sql import SparkSession, Window, SQLContext
from pyspark import SparkConf
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
		
	#@staticmethod
	#def total_match_count_by_adversaire(dfInput):
	#	cnt_cond = lambda cond: F.sum(F.when(cond, 1).otherwise(0))
	#	return dfInput.groupBy('adversaire').agg(
	#		cnt_cond(F.col('adversaire') == dfInput.adversaire).alias('match_count')
	#	)

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

	def step_one_clean_data(self, dfInput):
		dfInput = dfInput.withColumnRenamed("X4", "match").withColumnRenamed("X6", "competition")
		dfInput = dfInput.select("match", "competition", "adversaire", "score_france", "score_adversaire", "penalty_france", "penalty_adversaire", "date")
		dfInput = dfInput.withColumn('penalty_france', self.change_penality_null_by_0_udf(dfInput.penalty_france))
		dfInput = dfInput.withColumn('penalty_adversaire', self.change_penality_null_by_0_udf(dfInput.penalty_adversaire))
		dfInput = dfInput.withColumn('is_at_home', self.is_match_played_at_home_udf(dfInput.match, dfInput.adversaire))
		dfInput = dfInput.withColumn('is_at_world_cup', self.is_match_played_at_world_cup_udf(dfInput.competition))
		return self.filter_after_date(datetime(1980, 3, 1), dfInput)

	def step_two_generate_stats(self, dfMatches):
		windowByAdversaire = Window.partitionBy("adversaire")

		avgScoreFrance = F.avg("score_france").over(windowByAdversaire)
		avgScoreAdversaire = F.avg("score_adversaire").over(windowByAdversaire)
		totalMatchCount = F.count("adversaire").over(windowByAdversaire)
		rateMatchPlayedAtHomeCount = F.sum(F.col("is_at_home").cast("long")).over(windowByAdversaire) / F.count("adversaire").over(windowByAdversaire) * 100
		totalMatchWorldCup = F.sum(F.col("is_at_world_cup").cast("long")).over(windowByAdversaire)
		maxPenalityFrance = F.max("penalty_france").over(windowByAdversaire)
		maxPenalityAdversaire = F.max("penalty_adversaire").over(windowByAdversaire)
		
		dfStats = dfMatches.drop("competition").drop("match").drop("date")
		dfStats = dfStats.withColumn("avg_score_france", avgScoreFrance).drop("score_france")
		dfStats = dfStats.withColumn("avg_score_adversaire", avgScoreAdversaire).drop("score_adversaire")
		dfStats = dfStats.withColumn("match_count", totalMatchCount)
		dfStats = dfStats.withColumn("rate_at_home", rateMatchPlayedAtHomeCount).drop("is_at_home")
		dfStats = dfStats.withColumn("at_world_cup_count", totalMatchWorldCup).drop("is_at_world_cup")
		dfStats = dfStats.withColumn("max_penality_france", maxPenalityFrance).drop("penalty_france")
		dfStats = dfStats.withColumn("max_penalty_adversaire", maxPenalityAdversaire).drop("penalty_adversaire")
		dfStats.write.mode('overwrite').parquet("output/stats.parquet")
		#return dfStats.dropDuplicates()

	def main(self):
		dfMatches = self.load_dataFrame_from_csv("res/df_matches.csv")
		dfMatchesFiltered = self.step_one_clean_data(dfMatches)
		#dfMatchesFiltered.persist()

		self.step_two_generate_stats(dfMatchesFiltered)
		
		#dfStats.printSchema()
		#dfStats.show(50)

if __name__ == '__main__':
	footballApp = FootballApp(sys.argv)
	footballApp.main()