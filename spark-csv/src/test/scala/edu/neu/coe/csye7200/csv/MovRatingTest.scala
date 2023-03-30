package edu.neu.coe.csye7200.csv
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import scala.io.{Codec, Source}
import scala.util._
import org.scalatest.flatspec.AnyFlatSpec

import org.scalatest.matchers.must.Matchers

class MovRatingTest extends AnyFlatSpec with Matchers {
  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("test")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val Moviepath = "spark-csv/src/main/resources/movie_metadata.csv"
  val df = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(Moviepath)

  behavior of "MovRating"

  it should " Success for vaild csv path " in {
    Try(Source.fromResource(Moviepath)).isSuccess must be(true)
  }

  it should "Corect mean value" in {
    val mean = MovRating.Exdou(MovRating.RatingMean(df))
    mean must be(6.453200745804848)
  }
  it should "Corect stddev value" in {
    val stddev = MovRating.Exdou(MovRating.RatingStddev(df))
    stddev must be(0.9988071293753289)
  }
}
