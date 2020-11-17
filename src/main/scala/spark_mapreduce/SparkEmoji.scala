/*
All spark functions are within this class.
Each analytical question receives it's own method.
 */

package spark_mapreduce

import org.apache.spark.sql
import org.apache.spark.sql.functions.{desc, explode, not}
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
    import spark.implicits._
    val df = spark.read.option("multiline", multiline).json(path)

      if(!stream) {
        //getting the tweet DF
        val splitDfTweet = df.select("data").withColumn("data", functions.explode($"data"))
        val dfTweet = splitDfTweet.select("data.*")
        val dfTweetFlatten = dfTweet.toDF("author_id", "time", "id", "lang", "public_metrics", "text")
        val dfTweetFull = dfTweetFlatten.select("id", "author_id", "time", "lang", "public_metrics.*", "text")
        val dfTweetFullFlatten = dfTweetFull.toDF("tweet_id", "author_id", "time", "lang", "like_count", "quote_count", "reply_count", "retweet_count", "text")
        //dfTweetFullFlatten.show()

        //getting the author DF
        val splitDfAuth = df.select("includes.*")
        val dfAuthExpl = splitDfAuth.select("users").withColumn("users", functions.explode($"users"))
        val dfAuth = dfAuthExpl.select("users.id", "users.name", "users.username", "users.public_metrics")
        val dfAuthFlatten = dfAuth.toDF("id", "name", "username", "public_metrics")
        val dfAuthFull = dfAuthFlatten.select("id", "name", "username", "public_metrics.followers_count", "public_metrics.following_count", "public_metrics.tweet_count", "public_metrics.listed_count")
        val dfAuthFullFlatten = dfAuthFull.toDF("user_id", "name", "username", "followers_count", "following_count", "tweet_count", "listed_count")
        //dfAuthFullFlatten.show()

        //inner joins the two df
        dfRaw = dfTweetFullFlatten.join(dfAuthFullFlatten, dfTweetFullFlatten("author_id") === dfAuthFullFlatten("user_id"))
      }

    else{
      dfStreamRaw =  spark.readStream.schema(df.schema).json(path)
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

    val dfEmojiSplit = inputDF.select("tweet_id", "followers_count", "text")
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

    val dfEmojiSingle = emojiValue(inputDF)
    dfEmojiSingle.writeStream
      .outputMode("append")
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

  def popPeepsEmojisStream(df:DataFrame, threshold: Int, seconds: Int): Boolean ={
    import spark.implicits._
    val popEmojisDF = df.select($"followers_count",$"text")
      .filter( $"followers_count" > threshold)
      .groupBy("text")
      .count()
      .withColumnRenamed("count", "total")
      .orderBy(desc("total"))
    popEmojisDF.writeStream
      .outputMode("append")
      .format("console")
      .start()
      .awaitTermination(seconds*1000)
  }




}
