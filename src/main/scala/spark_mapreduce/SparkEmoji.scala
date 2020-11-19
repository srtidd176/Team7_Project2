/*
All spark functions are within this class.
Each analytical question receives it's own method.
 */

package spark_mapreduce

import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.functions.{desc, explode, not}
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}

import scala.collection.mutable.ListBuffer


class SparkEmoji(master: String) extends java.io.Serializable {
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
        dfRaw.show()
      }

    else{
        dfStreamRaw =  spark.readStream.schema(df.schema).json(path)
    }

  }


  /**
   * Takes a input DataFrame with raw Twitter Tweet values from a GetManyTweets request result and
   * produces a DataFrame containing rows with singular emojis from that data
   * @param inputDF the input DataFrame with raw values
   * @return DataFrame with only the emojis separated by spaces
   */


  def emojiValue(inputDF: DataFrame): DataFrame = {
      import spark.implicits._
      val df = rawDFtoEmojiDF(inputDF, false)
      df
    }
    /*
    val emojiRegexLike = "\u00a9|\u00ae|[\u2000-\u3300]|[\ud83c\ud000-\ud83c\udfff]|[\ud83d\ud000-\ud83d\udfff]|[\ud83e\ud000-\ud83e\udfff]" //Identify "emoji-like" words
      val emojiRegexSingle = "^\u00a9$|^\u00ae$|^[\u2000-\u3300]?[\uD83C\uDFFB-\uD83C\uDFFF]$|^[\ud83c\ud000-\ud83c\udfff]?[\uD83C\uDFFB-\uD83C\uDFFF]$|" +
        "^[\ud83d\ud000-\ud83d\udfff]?[\uD83C\uDFFB-\uD83C\uDFFF]$|^[\ud83e\ud000-\ud83e\udfff]?[\uD83C\uDFFB-\uD83C\uDFFF]$" //Identify unique emojis

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
  }*/

  /** TODO make it use rawDFtoEmojiDF
   * Takes a input DataFrame with raw Twitter Tweet values from a stream and produces a DataFrame
   * containing rows with singular emojis from that data
   * @param inputDF the input stream data file
   * @return a DataFrame with all required fields for emojis
   */
  def emojiValueStream(inputDF: DataFrame): DataFrame = {
   /* val emojiDF = rawDFtoEmojiDF(dfStreamRaw, true)
    emojiDF
  }*/
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


  /**
   * Show the top variations of the specified base emoji during a stream
   * @param df the DataFrame
   * @param baseEmoji the java unicode character for the BASE emoji
   * @param seconds seconds until the stream terminates
   * @return a boolean for true if it completes correctly
   */
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


  /**
   * TODO Finish the other fields and do it for the stream
   * Converts a raw DataFrame into a DataFrame with unique rows per exploded emoji
   * @param df the daw DataFrame containing a text column with emojis
   * @param isStream determines if using Twitter stream column names or not
   * @return a DataFrame with unique rows per exploded emoji
   */
  def rawDFtoEmojiDF(df: DataFrame, isStream: Boolean): DataFrame={
    import spark.implicits._
    var rows = df.rdd
    var dataFrameEmoji: DataFrame = df
    if(isStream) {
      rows = df.select("text","tweet_id", "author_id", "time", "lang", "like_count", "quote_count", "reply_count", "retweet_count", "user_id", "name", "username", "followers_count", "following_count", "tweet_count", "listed_count").rdd //add required stream rows here
      val rddList: RDD[Row] = rows.map(row => Row(breakUpEmojis(row.get(0).toString),row.get(1),row.get(2),row.get(3),row.get(4),row.get(5),row.get(6),row.get(7),row.get(8),row.get(9),row.get(10),row.get(11),row.get(12),row.get(13),row.get(14),row.get(15)))
      rddList.foreach(println(_))
      val schema = StructType(
        Seq(
          StructField(name = "text", dataType = ArrayType(StringType, true), nullable = false),
          StructField(name = "tweet_id", dataType = IntegerType, nullable = false),
          StructField(name = "author_id", dataType = IntegerType, nullable = false),
          StructField(name = "time", dataType = StringType, nullable = false),
          StructField(name = "lang", dataType = StringType, nullable = false),
          StructField(name = "like_count", dataType = IntegerType, nullable = false),
          StructField(name = "quote_count", dataType = IntegerType, nullable = false),
          StructField(name = "reply_count", dataType = IntegerType, nullable = false),
          StructField(name = "retweet_count", dataType = IntegerType, nullable = false),
          StructField(name = "user_id", dataType = IntegerType, nullable = false),
          StructField(name = "name", dataType = StringType, nullable = false),
          StructField(name = "username", dataType = StringType, nullable = false),
          StructField(name = "followers_count", dataType = IntegerType, nullable = false),
          StructField(name = "following_count", dataType = IntegerType, nullable = false),
          StructField(name = "tweet_count", dataType = IntegerType, nullable = false),
          StructField(name = "listed_count", dataType = IntegerType, nullable = false)
        )
      )
      dataFrameEmoji = spark.createDataFrame(rddList, schema).withColumn("text", functions.explode($"text"))

    }
    else{
      rows = df.select("text").rdd
      //,"tweet_id", "author_id", "time", "lang", "like_count", "quote_count", "reply_count", "retweet_count", "user_id", "name", "username", "followers_count", "following_count", "tweet_count", "listed_count").rdd //add required stream rows here
      val rddList: RDD[Row] = rows.map(row => Row(breakUpEmojis(row.get(0).toString)))//,row.get(1),row.get(2),row.get(3),row.get(4),row.get(5),row.get(6),row.get(7),row.get(8),row.get(9),row.get(10),row.get(11),row.get(12),row.get(13),row.get(14),row.get(15)))
      rddList.foreach(println(_))
      val schema = StructType(
        Seq(
          StructField(name = "text", dataType = ArrayType(StringType, true), nullable = false)/*,
          StructField(name = "tweet_id", dataType = IntegerType, nullable = false),
          StructField(name = "author_id", dataType = IntegerType, nullable = false),
          StructField(name = "time", dataType = StringType, nullable = false),
          StructField(name = "lang", dataType = StringType, nullable = false),
          StructField(name = "like_count", dataType = IntegerType, nullable = false),
          StructField(name = "quote_count", dataType = IntegerType, nullable = false),
          StructField(name = "reply_count", dataType = IntegerType, nullable = false),
          StructField(name = "retweet_count", dataType = IntegerType, nullable = false),
          StructField(name = "user_id", dataType = IntegerType, nullable = false),
          StructField(name = "name", dataType = StringType, nullable = false),
          StructField(name = "username", dataType = StringType, nullable = false),
          StructField(name = "followers_count", dataType = IntegerType, nullable = false),
          StructField(name = "following_count", dataType = IntegerType, nullable = false),
          StructField(name = "tweet_count", dataType = IntegerType, nullable = false),
          StructField(name = "listed_count", dataType = IntegerType, nullable = false)*/
        )
      )
      dataFrameEmoji = spark.createDataFrame(rddList, schema).withColumn("text", functions.explode($"text"))
 }

    dataFrameEmoji
  }


  /**
   * Finds and breaks up all emojis in a string into individual values
   * @param emoji the string containing the emojis
   * @return a List containing single emojis
   */
  def breakUpEmojis(emoji: String): List[String] = {
    val emojiRegexStage1 = "(\u00a9|\u00ae|[\u2000-\u3300]|\ud83c|\uD83D|\ud83e)".r // first base emoji char
    val emojiRegexStage2 = "([\ud83c\ud000-\ud83c\udfff]|[\ud83d\ud000-\ud83d\udfff]|[\ud83e\ud000-\ud83e\udfff])".r //second base emoji char
    val emojiRegexStage3 = "([\ud83d\uDC4B-\ud83d\udf82]\uD83C|[\ud83e\uD000-\ud83e\uDFFF]\uD83C)".r // skin tone identifier char
    val emojiRegexStage4 = "([\ud83d\uDC4B-\ud83d\udf82][\uD83C\uDFFB-\uD83C\uDFFF]|[\ud83e\uD000-\ud83e\uDFFF][\uD83C\uDFFB-\uD83C\uDFFF])".r // specific color identifier char
    val letterRegex = "(\\w|\\s|[.,’'\\/#!+“<>”\"?$@%\\^&\\*;:{}=\\-_`~()])".r // ignore regex
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
    outputList.toList
  }



}
