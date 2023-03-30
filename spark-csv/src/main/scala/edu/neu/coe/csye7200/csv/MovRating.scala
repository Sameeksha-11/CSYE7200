package edu.neu.coe.csye7200.csv

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
object MovRating extends App{

  val Moviepath = "spark-csv/src/main/resources/movie_metadata.csv"
  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Spark CSV Reader")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(Moviepath)

  def dfshow(): Unit = {
    df.show(false)
  }


  def RatingMean(Dfin : DataFrame): DataFrame = {

    Dfin.select(avg("imdb_score"))

  }
  def RatingStddev(Dfin : DataFrame): DataFrame = {

    Dfin.select(stddev("imdb_score"))

  }



  def Exdou(Df: DataFrame): Double =
    {
  Df.collect() match {
    case Array(row) => row.getDouble(0)
    case _ => 0.0
  }
  }


  dfshow()
  val df1 = RatingMean(df)
  df1.show()
  println("Mean : ", Exdou(df1))
  val df2 = RatingStddev(df)
  df2.show()
  println("Stddev : ", Exdou(df2))

}
