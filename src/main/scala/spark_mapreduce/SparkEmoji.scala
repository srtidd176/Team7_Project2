/*
All spark functions are within this class.
Each analytical question receives it's own method.
 */

package spark_mapreduce

import org.apache.spark.sql
import org.apache.spark.sql.functions.{desc, explode, not}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import scala.collection.mutable.ListBuffer


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
   * Takes a input DataFrame with raw Twitter Tweet values from a GetManyTweets request result and
   * produces a DataFrame containing rows with singular emojis from that data
   * @param inputDF the input DataFrame with raw values
   * @return DataFrame with only the emojis separated by spaces
   */

    //TODO may make more sense to take in a DataFrame than a path
    def emojiValue(inputDF: DataFrame): DataFrame ={
      import spark.implicits._

      val emojiRegexLike = "\u00a9|\u00ae|[\u2000-\u3300]|[\ud83c\ud000-\ud83c\udfff]|[\ud83d\ud000-\ud83d\udfff]|[\ud83e\ud000-\ud83e\udfff]" //Identify "emoji-like" words
      val emojiRegexSingle = "^\u00a9$|^\u00ae$|^[\u2000-\u3300]$|^[\ud83c\ud000-\ud83c\udfff]$|^[\ud83d\ud000-\ud83d\udfff]$|^[\ud83e\ud000-\ud83e\udfff]$" //Identify unique emojis

      val dfEmojiSplit = inputDF.select("tweet_id", "followers_count", "text", "lang", "like_count", "retweet_count")
        .withColumn("text", functions.explode(functions.split($"text", "\\s"))) //split by spaces and explode
        .filter($"text" rlike emojiRegexSingle) // filter out everything that is not emoji-like

      val condition = $"text" rlike emojiRegexSingle //filter out everything that is not a single emoji
      val dfEmojiSingle = dfEmojiSplit.filter(condition) //single emojis
      val dfEmojiGroups = dfEmojiSplit.filter(not(condition)) //concatenated emojis

      //dfEmojiGroups.show()
      //dfEmojiSingle.show()

      dfEmojiSplit //return

      //TODO Break up the emoji groups
      /*val emojiGroupsRDD = dfEmojiGroups//.withColumn("text", functions.explode(functions.split($"text", emojiRegexSplit)))
        .filter(condition) //collect single emojis
        .filter($"text" rlike "(?=[^?])") //ignore any unknown items
        .show(50)
      */
    }


  /**
   * Takes a input DataFrame with raw Twitter Tweet values from a stream and produces a DataFrame
   * containing rows with singular emojis from that data
   * @param inputDF the input stream data file
   * @return a DataFrame with all required fields for emojis
   */
  def emojiValueStream(inputDF: DataFrame): DataFrame ={
    import spark.implicits._
    val emojiRegexLike = "\u00a9|\u00ae|[\u2000-\u3300]|[\ud83c\ud000-\ud83c\udfff]|[\ud83d\ud000-\ud83d\udfff]|[\ud83e\ud000-\ud83e\udfff]" //Identify "emoji-like" words
    val emojiRegexSingle = "^\u00a9$|^\u00ae$|^[\u2000-\u3300]?[\uD83C\uDFFB-\uD83C\uDFFF]$|^[\ud83c\ud000-\ud83c\udfff]?[\uD83C\uDFFB-\uD83C\uDFFF]$|" +
      "^[\ud83d\ud000-\ud83d\udfff]?[\uD83C\uDFFB-\uD83C\uDFFF]$|^[\ud83e\ud000-\ud83e\udfff]?[\uD83C\uDFFB-\uD83C\uDFFF]$" //Identify unique emojis

    //TODO Add all required columns in the select bellow
    val dfEmojiSplit = inputDF.select("data.id", "includes.users.public_metrics.followers_count", "data.text")
      .withColumn("text", functions.explode(functions.split($"text", "\\s"))) //split by spaces and explode
      .filter($"text" rlike emojiRegexLike) // filter out everything that is not emoji-like

    val condition = $"text" rlike emojiRegexSingle //filter out everything that is not a single emoji
    val dfEmojiSingle = dfEmojiSplit.filter(condition) //single emojis
    val dfEmojiGroups = dfEmojiSplit.filter(not(condition)) //concatenated emojis

    dfEmojiSingle
    //dfEmojiSingle
    //dfEmojiGroups
    //TODO Break up the emoji groups
   // val emojiGroupsSplit = dfEmojiGroups
    //  .withColumn("text", functions.explode(functions.split($"text", "")))
      //.filter($"text" rlike "\\W")
    //.filter(condition) //collect single emojis
     //emojiGroupsSplit
  }

  /**
   * Compares the top emojis of two different languages
   * @param df the input raw data frame
   * @param language1 the first language you are searching for emojis, will be ordered by this languages top emojis
   * @param language2 the second language you are searching for emojis
   */
  def langTopEmojisHist(df: DataFrame, language1: String, language2: String): Unit ={
    import spark.implicits._
    val langEmojiDF = emojiValue(df)
    val lang1EmojisDF = langEmojiDF.select("lang","text")
      .filter( $"lang" === language1)
      .groupBy("text")
      .count()
      .withColumnRenamed("count", s"${language1} total")
      .orderBy(desc(s"${language1} total"))
    val lang2EmojisDF = langEmojiDF.select("lang","text")
      .filter( $"lang" === language2)
      .groupBy("text")
      .count()
      .withColumnRenamed("count", s"${language2} total")
    val fullLangEmojisDF = lang1EmojisDF.join(lang2EmojisDF, lang1EmojisDF("text") === lang2EmojisDF("text"), "fullouter")
      .orderBy(desc(s"${language1} total"))
    fullLangEmojisDF.show()
  }

  /**
   * Shows the top emojis used by most liked or retweeted tweets
   * @param df the input raw data frame
   * @param likes true for likes, false for retweets
   */
  def popTweetsEmojiHist(df: DataFrame, likes: Boolean): Unit = {
    import spark.implicits._
    val tweetEmojiDF = emojiValue(df)
    if(likes == true) {
      val likeEmojiDF = tweetEmojiDF.select("text", "like_count")
        .groupBy("text")
        .avg("like_count")
        .withColumnRenamed("avg(like_count)", "average_likes")
        .orderBy(desc("total_likes"))
      likeEmojiDF.show()
    }
    else {
      val likeEmojiDF = tweetEmojiDF.select("text", "retweet_count")
        .groupBy("text")
        .avg("retweet_count")
        .withColumnRenamed("avg(retweet_count)", "average_retweets")
        .orderBy(desc("total_retweets"))
      likeEmojiDF.show()
    }
  }

  /**
   * Shows the top emojis used by famous people
   * @param df the input raw data frame
   * @param threshold the minimum number of followers required to be "famous"
   * @param seconds the duration of the stream feed
   * @return a boolean for true if it completes correctly
   */
  def popPeepsEmojisStream(df:DataFrame, threshold: Int, seconds: Int): Boolean ={
    import spark.implicits._
    val popEmojisDF = df.select("followers_count","text")
      .filter( $"followers_count"(0) > threshold)
      .groupBy("text")
      .count()
      .withColumnRenamed("count", "total")
      .orderBy(desc("total"))
    popEmojisDF.writeStream
      .outputMode("complete")
      .format("console")
      .start()
      .awaitTermination(seconds*1000)
  }


  def topEmojiVariationStream(df: DataFrame, baseEmoji: String, seconds: Int): Boolean ={
    /*
    light skin: \uD83C\uDFFB
    medium-light skin: \uD83C\uDFFC
    medium skin: \uD83C\uDFFD
    medium-dark skin: \uD83C\uDFFE
    dark skin: \uD83C\uDFFF
    */
    import spark.implicits._
    val emojiVariation = df.select("text")
      .filter($"text" rlike baseEmoji)
      .groupBy("text")
      .count()
      .withColumnRenamed("count", "total")
      .orderBy(desc("total"))
    emojiVariation.writeStream
      .outputMode("complete")
      .format("console")
      .start()
      .awaitTermination(seconds*1000)
  }

  def breakUpEmojis(emoji: String): ListBuffer[String] = {
    val emojiRegexStage1 = "(\u00a9|\u00ae|[\u2000-\u3300]|\ud83c|\uD83D|\ud83e)".r // first base emoji char
    val emojiRegexStage2 = "([\ud83c\ud000-\ud83c\udfff]|[\ud83d\ud000-\ud83d\udfff]|[\ud83e\ud000-\ud83e\udfff])".r //second base emoji char
    val emojiRegexStage3 = "([\ud83d\uDC4B-\ud83d\udf82]\uD83C|[\ud83e\uD000-\ud83e\uDFFF]\uD83C)".r // skin tone identifier char
    val emojiRegexStage4 = "([\ud83d\uDC4B-\ud83d\udf82][\uD83C\uDFFB-\uD83C\uDFFF]|[\ud83e\uD000-\ud83e\uDFFF][\uD83C\uDFFB-\uD83C\uDFFF])".r // specific color identifier char
    val letterRegex = "(\\w|\\s|[.,’'\\/#!$@%\\^&\\*;:{}=\\-_`~()])".r
    var oldPosEmoji = ""
    var newPosEmoji = ""
    var baseEmojiBackup = ""
    val outputList: ListBuffer[String] = ListBuffer()

    (emoji + "|") // adds this symbol to indicate to print the last emoji in the string
      .split("").foreach(f => {
      newPosEmoji = oldPosEmoji + f
      f match {
        case letterRegex(c) => // Do nothing with letters
        case _ => {
          newPosEmoji match {
            case emojiRegexStage1(c) => { // first base emoji char
              oldPosEmoji = newPosEmoji
            }
            case emojiRegexStage2(c) => { // second base emoji char
              oldPosEmoji = newPosEmoji
            }
            case emojiRegexStage3(c) => { // skin tone identifier char
              baseEmojiBackup = oldPosEmoji // make a backup of the previous emoji to handle use cases of a base emoji followed by an emoji starting with \uD83C
              oldPosEmoji = newPosEmoji
            }
            case emojiRegexStage4(c) => { // specific color identifier char
              oldPosEmoji = newPosEmoji
              //println(oldPosEmoji)
              outputList += oldPosEmoji
              oldPosEmoji = ""
              baseEmojiBackup = ""
            }
            case _ => {
              baseEmojiBackup match{
                case emojiRegexStage2(c) if(c != "") => { // determine if the backup of the base emoji  is needed
                  //println(c)
                  outputList += c
                  baseEmojiBackup = ""
                  oldPosEmoji = "\uD83C"+f
                }
                case _ => {
                  //println(oldPosEmoji) // if it is no longer an emoji then just refresh oldPosEmoji
                  outputList += oldPosEmoji
                  oldPosEmoji = f
                }
              }
            }
          }
        }
      }
    })
    outputList
  }



}
