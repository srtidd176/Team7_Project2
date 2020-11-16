package spark_mapreduce

import org.apache.spark.sql.{SparkSession, functions}

class Functions {

  val se = new SparkEmoji("local[4]")

  def tweetDF(df2: Int, spark: SparkSession, jsonFile: String): Unit = {
    import spark.implicits._

    val df = spark.read.json(jsonFile)

    //getting the tweet DF
    val splitDfTweet = df.select("data").withColumn("data", functions.explode($"data"))
    val dfTweet = splitDfTweet.select("data.*")
    val dfTweetFlatten = dfTweet.toDF("author_id", "time", "id", "lang", "public_metrics", "text")
    val dfTweetFull = dfTweetFlatten.select("id", "author_id", "time", "lang", "public_metrics.*", "text")
    val dfTweetFullFlatten = dfTweetFull.toDF("id", "author_id", "time", "lang", "like_count", "quote_count", "reply_count", "retweet_count", "text")
    //dfTweetFullFlatten.show()

    //getting the author DF
    val splitDfAuth = df.select("includes.*")
    val dfAuthExpl = splitDfAuth.select("users").withColumn("users", functions.explode($"users"))
    val dfAuth = dfAuthExpl.select("users.id", "users.name", "users.username", "users.public_metrics")
    val dfAuthFlatten = dfAuth.toDF("id", "name", "username", "public_metrics")
    val dfAuthFull = dfAuthFlatten.select("id", "name", "username", "public_metrics.followers_count", "public_metrics.following_count", "public_metrics.tweet_count", "public_metrics.listed_count")
    val dfAuthFullFlatten = dfAuthFull.toDF("id", "name", "username", "followers_count", "following_count", "tweet_count", "listed_count")
    //dfAuthFullFlatten.show()

    //inner joins the two df
    val dfFull = dfTweetFullFlatten.join(dfAuthFullFlatten, dfTweetFullFlatten("author_id") === dfAuthFullFlatten("id"))

    dfFull.show()

  }


}
