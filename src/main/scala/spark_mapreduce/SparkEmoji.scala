/*
All spark functions are within this class.
Each analytical question receives it's own method.
 */

package spark_mapreduce

import org.apache.spark.sql
import org.apache.spark.sql.functions.{explode, not}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}


class SparkEmoji(master: String) {
  // Initialize Spark

  var dfRaw: sql.DataFrame = null
  var dfStreamRaw: sql.DataFrame = null
  var emojiDF: sql.DataFrame = null

  val spark = SparkSession.builder()
    .appName("Twitter Emoji Analysis")
    .master(master)
    .getOrCreate()

  //Set log level to WARN
  spark.sparkContext.setLogLevel("WARN")


  /**
   * Uploads the JSON file/directory as a DataFrame
   * @param path The path to the file
   * @param multiline boolean to indicate whether the JSON objects are multilined or not
   */
  def uploadJSON(path: String, multiline: Boolean, stream: Boolean): Unit={
    dfRaw = spark.read.option("multiline", multiline).json(path)

    if(stream){
      dfStreamRaw =  spark.readStream.schema(dfRaw.schema).json(path)
    }

  }

  /**
   * Shows the raw data from the uploaded DataFrame
   */
  def dfRawShow(): Unit={
    dfRaw.explain()
    dfRaw.show()
  }

  /**
   * Takes a input DataFrame with raw Twitter Tweet values and produces a DataFrame containing rows with singular emojis from that data
   * @param inputDF the input DataFrame with raw values
   * @return DataFrame with only the emojis separated by spaces
   */

    //TODO may make more sense to take in a DataFrame than a path
  def emojiValue(inputDF: DataFrame): DataFrame ={
    import spark.implicits._

    val emojiRegexLike = "\u00a9|\u00ae|[\u2000-\u3300]|[\ud83c\ud000-\ud83c\udfff]|[\ud83d\ud000-\ud83d\udfff]|[\ud83e\ud000-\ud83e\udfff]" //Identify "emoji-like" words
    val emojiRegexSingle = "^\u00a9$|^\u00ae$|^[\u2000-\u3300]$|^[\ud83c\ud000-\ud83c\udfff]$|^[\ud83d\ud000-\ud83d\udfff]$|^[\ud83e\ud000-\ud83e\udfff]$" //Identify unique emojis

    val dfEmojiSplit = inputDF.select("id", "text")
      .withColumn("text", functions.explode(functions.split($"text", "\\s"))) //split by spaces and explode
      .filter($"text" rlike emojiRegexLike) // filter out everything that is not emoji-like

    val condition = $"text" rlike emojiRegexSingle //filter out everything that is not a single emoji
    val dfEmojiSingle = dfEmojiSplit.filter(condition) //single emojis
    val dfEmojiGroups = dfEmojiSplit.filter(not(condition)) //concatenated emojis

    //dfEmojiGroups.show()
    //dfEmojiSingle.show()

    dfEmojiSingle //return

    //TODO Break up the emoji groups
    /*val emojiGroupsRDD = dfEmojiGroups//.withColumn("text", functions.explode(functions.split($"text", emojiRegexSplit)))
      .filter(condition) //collect single emojis
      .filter($"text" rlike "(?=[^?])") //ignore any unknown items
      .show(50)
    */
  }

  def emojiValueStream(inputDF: DataFrame, seconds: Int): Boolean ={
    import spark.implicits._

    val emojiRegexLike = "\u00a9|\u00ae|[\u2000-\u3300]|[\ud83c\ud000-\ud83c\udfff]|[\ud83d\ud000-\ud83d\udfff]|[\ud83e\ud000-\ud83e\udfff]" //Identify "emoji-like" words
    val emojiRegexSingle = "^\u00a9$|^\u00ae$|^[\u2000-\u3300]$|^[\ud83c\ud000-\ud83c\udfff]$|^[\ud83d\ud000-\ud83d\udfff]$|^[\ud83e\ud000-\ud83e\udfff]$" //Identify unique emojis

    val dfEmojiSplit = inputDF.select("data.id", "data.text")
      .withColumn("text", functions.explode(functions.split($"text", "\\s"))) //split by spaces and explode
      .filter($"text" rlike emojiRegexLike) // filter out everything that is not emoji-like

    val condition = $"text" rlike emojiRegexSingle //filter out everything that is not a single emoji
    val dfEmojiSingle = dfEmojiSplit.filter(condition) //single emojis
    val dfEmojiGroups = dfEmojiSplit.filter(not(condition)) //concatenated emojis

    dfEmojiSingle.writeStream
      .outputMode("complete")
      .format("console")
      .start()
      .awaitTermination(seconds*1000)

    //TODO Break up the emoji groups
    /*val emojiGroupsRDD = dfEmojiGroups//.withColumn("text", functions.explode(functions.split($"text", emojiRegexSplit)))
      .filter(condition) //collect single emojis
      .filter($"text" rlike "(?=[^?])") //ignore any unknown items
      .show(50)
    */
  }




}
