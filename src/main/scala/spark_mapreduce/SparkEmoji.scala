/*
All spark functions are within this class.
Each analytical question receives it's own method.
 */

package spark_mapreduce

import org.apache.spark.sql
import org.apache.spark.sql.{SparkSession, functions}


class SparkEmoji(master: String) {
  // Initialize Spark

  var dfRaw: sql.DataFrame = null

  val spark = SparkSession.builder()
    .appName("Twitter Emoji Analysis")
    .master(master)
    .getOrCreate()

  //Set log level to WARN
  spark.sparkContext.setLogLevel("WARN")


  /**
   * Uploads the JSON file as a DataFrame
   * @param path The path to the file
   * @param multiline boolean to indicate whether the JSON objects are multilined or not
   */
  def uploadJSON(path: String, multiline: Boolean): Unit={
    dfRaw = spark.read.option("multiline", multiline).json(path)
  }

  /**
   * Shows the raw data from the uploaded DataFrame
   */
  def dfRawShow(): Unit={
    dfRaw.explain()
    dfRaw.show()
  }

  //TODO delete this example
  /**
   * Shows the average age by eye color for people with first name of length < lenMax
   * @param lenMax The max number for first name length
   */
  def demoQuery(lenMax: Int): Unit ={
    val demoQuery = dfRaw.filter(functions.length(dfRaw("name.first")) < lenMax)
      .groupBy("eyeColor")
      .agg(functions.avg("age"))
    demoQuery.show()
  }




}