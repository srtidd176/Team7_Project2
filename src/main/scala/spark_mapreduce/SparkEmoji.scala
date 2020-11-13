/*
All spark functions are within this class.
Each analytical question receives it's own method.
 */

package spark_mapreduce

import org.apache.spark.sql
import org.apache.spark.sql.functions.{explode, not}
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

  def emojiValue(): Unit ={
    import spark.implicits._

    val emojiRegexSplit = "\u00a9|\u00ae|[\u2000-\u3300]|[\ud83c\ud000-\ud83c\udfff]|[\ud83d\ud000-\ud83d\udfff]|[\ud83e\ud000-\ud83e\udfff]"
    val emojiRegexSingle = "^\u00a9$|^\u00ae$|^[\u2000-\u3300]$|^[\ud83c\ud000-\ud83c\udfff]$|^[\ud83d\ud000-\ud83d\udfff]$|^[\ud83e\ud000-\ud83e\udfff]$"
    uploadJSON("twitter.json",true)


    val dfEmojiSplit = dfRaw.select("id", "text")
      .withColumn("text", functions.explode(functions.split($"text", "\\s"))) //split by spaces and explode
      .filter($"text" rlike emojiRegexSplit) // filter out everything that is not emoji-like

    val condition = $"text" rlike emojiRegexSingle //filter out everything that is not a single emoji
    val dfEmojiSingle = dfEmojiSplit.filter(condition) //single emojis
    val dfEmojiGroups = dfEmojiSplit.filter(not(condition)) //concatenated emojis

    dfEmojiGroups.show()
    dfEmojiSingle.show()

    //TODO Break up the emoji groups
    //dfEmojiGroups.foreach( row => row.get(1))

    //dfEmojiGroups.withColumn("text", functions.explode(functions.split($"text", emojiRegexSplit)))
      //.filter(condition) //collect single emojis
      //.filter($"text" rlike "(?=[^?])") //ignore any unknown items
      //.show()


    //dfRaw.printSchema()

    //dfEmojiSplit.show()
  }



}
