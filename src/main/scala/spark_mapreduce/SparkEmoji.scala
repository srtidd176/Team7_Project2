/*
All spark functions are within this class.
Each analytical question receives it's own method.
 */

package spark_mapreduce

import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.functions.{dayofweek, days, desc, explode, hour, not, to_timestamp, to_utc_timestamp, udf, when}
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, StringType, StructField, StructType}
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
        val dfTweetFlatten = dfTweet.toDF("author_id", "time", "id", "lang", "public_metrics", "text", "withheld")
        val dfTweetFull = dfTweetFlatten.select("id", "author_id", "time", "lang", "public_metrics.*", "text")
        val dfTweetFullFlatten = dfTweetFull.toDF("tweet_id", "author_id", "time", "lang", "like_count", "quote_count", "reply_count", "retweet_count", "text")
//        dfTweetFullFlatten.show()

        //getting the author DF
        val splitDfAuth = df.select("includes.*")
        val dfAuthExpl = splitDfAuth.select("users").withColumn("users", functions.explode($"users"))
        val dfAuth = dfAuthExpl.select("users.id", "users.name", "users.username", "users.public_metrics")
        val dfAuthFlatten = dfAuth.toDF("id", "name", "username", "public_metrics")
        val dfAuthFull = dfAuthFlatten.select("id", "name", "username", "public_metrics.followers_count", "public_metrics.following_count", "public_metrics.tweet_count", "public_metrics.listed_count")
        val dfAuthFullFlatten = dfAuthFull.toDF("user_id", "name", "username", "followers_count", "following_count", "tweet_count", "listed_count")
//        dfAuthFullFlatten.show()

        //inner joins the two df
        dfRaw = dfTweetFullFlatten.join(dfAuthFullFlatten, dfTweetFullFlatten("author_id") === dfAuthFullFlatten("user_id"))
      }

    else{
        dfStreamRaw =  spark.readStream.schema(df.schema).json(path)
    }

  }

  /**
   * topEmojisOfWeek - outputs the top emojis of the week.
   */
  def topEmojisOfWeek() ={
    rawDFtoEmojiDF(dfRaw)
      .select("text")
      .groupBy("text")
      .count()
      .withColumnRenamed("text", "emoji")
      .withColumnRenamed("count", "usage")
      .orderBy(desc("count"))
      .show()
  }

  /**
   * popEmojiByTime - outputs to console returns most popular emojies by daily, hourly, dailyAndHourly
   * @param opt : String - daily - top emojis of a day
   *                       hourly - top emojis by hour across days
   *                       dailyAndHourly - top emojis by each hour of a day
   */
  def popEmojiByTime(opt:String ="dailyAndHourly"): Unit = {
    import spark.implicits._

    val dateEmoji = rawDFtoEmojiDF(dfRaw)
      .withColumn("time", to_utc_timestamp( $"time", "UTC" ) )
      .withColumn("dayOfWeek", dayofweek($"time"))
      .withColumn("hour", hour($"time"))
      .select("text", "time", "dayOfWeek", "hour")

    val countedEmoji = dateEmoji
      .groupBy("dayOfWeek", "hour", "text")
      .count()

    opt match {
      case "daily" => {
        val dailyEmojis = countedEmoji
          .groupBy("dayOfWeek" , "text")
          .sum("count")

        dailyEmojis.as("max")
          .groupBy("dayOfWeek")
          .max("sum(count)").as("sum")
          .join(dailyEmojis, dailyEmojis("dayOfWeek") === $"sum.dayOfWeek" && dailyEmojis("sum(count)") === $"max(sum(count))")
          .select( dailyEmojis("dayOfWeek"), $"text".as("Emoji"), $"sum(count)".as("Popularity"))
          .orderBy($"dayOfWeek")
          .withColumn("dayOfWeek", when(dailyEmojis("dayOfWeek") === 1, "Monday")
          .when(dailyEmojis("dayOfWeek") === 2, "Tuesday")
            .when(dailyEmojis("dayOfWeek") === 3, "Wednesday")
            .when(dailyEmojis("dayOfWeek") === 4, "Thursday")
            .when(dailyEmojis("dayOfWeek") === 5, "Friday")
            .when(dailyEmojis("dayOfWeek") === 6, "Saturday")
            .when(dailyEmojis("dayOfWeek") === 7, "Sunday")
          .otherwise("OppsieThatIsNotDay"))
          .show(false)
      }
      case "hourly" => {
        val hourlyEmojis = countedEmoji
          .groupBy("hour" , "text")
          .sum("count")

        hourlyEmojis.as("hour")
          .groupBy("hour")
          .max("sum(count)")
          //      .orderBy($"dayOfWeek", $"hour", desc("max(count)") )
          .join(
            hourlyEmojis,
            hourlyEmojis("sum(count)") === $"max(sum(count))"
              && hourlyEmojis("hour") === $"hour.hour")
          .select( $"hour.hour", $"text", $"sum(count)")
          .withColumnRenamed("text", "MostPopEmoji")
          .withColumnRenamed("sum(count)","Occurance")
          .orderBy("hour")
          .show(24,false)

    }
      case "dailyAndHourly" => {
        val dailyEmojis = countedEmoji
          .groupBy("dayOfWeek" , "hour", "text")
          .sum("count").withColumnRenamed("sum(count)", "count")

        dailyEmojis.as("dayAndHour")
          .groupBy("dayOfWeek","hour")
          .max("count")
          //      .orderBy($"dayOfWeek", $"hour", desc("max(count)") )
          .join(dailyEmojis,
            dailyEmojis("dayOfWeek") === $"dayAndHour.dayOfWeek"
              && dailyEmojis("count") === $"max(count)"
              && dailyEmojis("hour") === $"dayAndHour.hour")
          .select( $"dayAndHour.dayOfWeek", $"dayAndHour.hour", $"text", $"count")
          .orderBy("dayOfWeek", "hour")
          .withColumn("dayOfWeek", when(dailyEmojis("dayOfWeek") === 1, "Monday")
            .when(dailyEmojis("dayOfWeek") === 2, "Tuesday")
            .when(dailyEmojis("dayOfWeek") === 3, "Wednesday")
            .when(dailyEmojis("dayOfWeek") === 4, "Thursday")
            .when(dailyEmojis("dayOfWeek") === 5, "Friday")
            .when(dailyEmojis("dayOfWeek") === 6, "Saturday")
            .when(dailyEmojis("dayOfWeek") === 7, "Sunday")
            .otherwise("OppsieThatIsNotDay"))
          .show(168,false)

      }
    }

  }

  /**
   * Compares the top emojis of two different languages
   * @param df the input raw data frame
   * @param language1 the first language you are searching for emojis, will be ordered by this languages top emojis
   * @param language2 the second language you are searching for emojis
   */
  def langTopEmojisHist(df: DataFrame, language1: String, language2: String): Unit ={
    import spark.implicits._
    val langEmojiDF = rawDFtoEmojiDF(df)
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
    val tweetEmojiDF = rawDFtoEmojiDF(df)
    if(likes == true) {
      val likeEmojiDF = tweetEmojiDF.select("text", "like_count")
        .groupBy("text")
        .avg("like_count")
        .withColumnRenamed("avg(like_count)", "average_likes")
        .orderBy(desc("average_likes"))
      likeEmojiDF.show()
    }
    else {
      val likeEmojiDF = tweetEmojiDF.select("text", "retweet_count")
        .groupBy("text")
        .avg("retweet_count")
        .withColumnRenamed("avg(retweet_count)", "average_retweets")
        .orderBy(desc("average_retweets"))
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
    val popEmojisDF = df.distinct().select("followers_count","text")
      .filter( $"followers_count"(0) > threshold)
      .groupBy("text")
      .count()
      .withColumnRenamed("count", "total")
      .orderBy(desc("total"))
    popEmojisDF.select("text", "total").writeStream
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
    val emojiVariation = df.distinct().select("text")
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
   * Converts a raw STRUCTURED STREAM DataFrame into a DataFrame with unique rows per exploded emoji
   * @param df the raw stream DataFrame containing a text column with emojis
   * @return a DataFrame with unique rows per exploded emoji
   */
  def rawDFtoEmojiDFStream(df: DataFrame): DataFrame={
  import spark.implicits._
  val emojiBreakUpUDF = udf((string: String) => breakUpEmojis(string))
  spark.udf.register("emojiBreakUpUDF",emojiBreakUpUDF)
  val  rows = df.select("data.text","data.id", "data.author_id", "data.created_at", "data.lang", "data.public_metrics.like_count", "data.public_metrics.quote_count",
      "data.public_metrics.reply_count", "data.public_metrics.retweet_count", "includes.users.name", "includes.users.username", "includes.users.public_metrics.followers_count", "includes.users.public_metrics.following_count",
      "includes.users.public_metrics.tweet_count", "includes.users.public_metrics.listed_count")
    //author_id, followers_count, following_count, lang, like_count, listed_count, name, quote_count, reply_count, retweet_count, text, time, tweet_count, tweet_id, user_id, username
  val dfEmojis = rows.withColumn("text", functions.explode(emojiBreakUpUDF($"text")))
  dfEmojis
}

  /**
   * Converts a raw HISTORIC DataFrame into a DataFrame with unique rows per exploded emoji
   * @param df the raw historic DataFrame containing a text column with emojis
   * @return a DataFrame with unique rows per exploded emoji
   */
  def rawDFtoEmojiDF(df: DataFrame): DataFrame={
    import spark.implicits._
    var rows = df.rdd
    var dataFrameEmoji: DataFrame = df
      rows = df.select("text","tweet_id", "author_id", "time", "lang", "like_count", "quote_count",
        "reply_count", "retweet_count", "user_id", "name", "username", "followers_count", "following_count",
      "tweet_count", "listed_count").rdd
      //author_id, followers_count, following_count, lang, like_count, listed_count, name, quote_count, reply_count, retweet_count, text, time, tweet_count, tweet_id, user_id, username
      val rddList: RDD[Row] = rows.map(row => Row(breakUpEmojis(row.get(0).toString),row.get(1),row.get(2),row.get(3),
        row.get(4),row.get(5),row.get(6),row.get(7),row.get(8),row.get(9),row.get(10),row.get(11),row.get(12),row.get(13),
        row.get(14),row.get(15)))
//      rddList.foreach(println(_))
      val schema = StructType(
        Seq(
          StructField(name = "text", dataType = ArrayType(StringType, true), nullable = false),
          StructField(name = "tweet_id", dataType = StringType, nullable = false),
          StructField(name = "author_id", dataType = StringType, nullable = false),
          StructField(name = "time", dataType = StringType, nullable = false),
          StructField(name = "lang", dataType = StringType, nullable = false),
          StructField(name = "like_count", dataType = LongType, nullable = false),
          StructField(name = "quote_count", dataType = LongType, nullable = false),
          StructField(name = "reply_count", dataType = LongType, nullable = false),
          StructField(name = "retweet_count", dataType = LongType, nullable = false),
          StructField(name = "user_id", dataType = StringType, nullable = false),
          StructField(name = "name", dataType = StringType, nullable = false),
          StructField(name = "username", dataType = StringType, nullable = false),
          StructField(name = "followers_count", dataType = LongType, nullable = false),
          StructField(name = "following_count", dataType = LongType, nullable = false),
          StructField(name = "tweet_count", dataType = LongType, nullable = false),
          StructField(name = "listed_count", dataType = LongType, nullable = false)
        )
      )
      dataFrameEmoji = spark.createDataFrame(rddList, schema).withColumn("text", functions.explode($"text"))
        .withColumn("text", functions.explode(functions.split($"text", "\\s")))
    dataFrameEmoji
  }


  /**
   * Finds and breaks up all emojis in a string into individual values
   * @param emoji the string containing the emojis
   * @return an Array containing single emojis
   */
  def breakUpEmojis(emoji: String): Array[String] = {
    val emojiRegexStage1 = "(\u00a9|\u00ae|[\u2000-\u3300]|\ud83c|\uD83D|\ud83e)".r // first base emoji char
    val emojiRegexStage2 = "([\ud83c\ud000-\ud83c\udfff]|[\ud83d\ud000-\ud83d\udfff]|[\ud83e\ud000-\ud83e\udfff])".r //second base emoji char
    val emojiRegexStage3 = "([\ud83d\uDC4B-\ud83d\udf82]\uD83C|[\ud83e\uD000-\ud83e\uDFFF]\uD83C)".r // skin tone identifier char
    val emojiRegexStage4 = "([\ud83d\uDC4B-\ud83d\udf82][\uD83C\uDFFB-\uD83C\uDFFF]|[\ud83e\uD000-\ud83e\uDFFF][\uD83C\uDFFB-\uD83C\uDFFF])".r // specific color identifier char
    val letterRegex = ("(\\w|\\s|\u0000|[\u0000-\u00a8]|[\u00aa-\u00ad]|[\u00af-\u1999]|[\u0621-\u064A]|[\u3040-ゟ]|" +
      "[゠-㆟]|[\uFF00-｝]|[\uD83C\uDDF7에\uD83C\uDDF0━\uD83C\uDDEB】   『    …    뮤이  　。   ️스”【、   ─“…」「]|[～-\uFFEF]|" +
      "[一-龠]|[ぁ-ゔ]| [ァ-ヴー]|[々〆〤]|[.,’'\\/#?!$@%\\^&\\*;:{}=\\-_`~()])").r
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
    outputList.toArray
  }



}
