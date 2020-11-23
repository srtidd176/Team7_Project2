package spark_mapreduce

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object Runner {

  val sparkEmoji: SparkEmoji = new SparkEmoji("local[4]")
  val twitterApi: TwitterApi = new TwitterApi(System.getenv("TWITTER_BEARER_TOKEN"))

  def main(args: Array[String]): Unit = {
    args match {

      // Currently Helper to pull Recent Tweets by Time interval for Historical Records
      case Array(func, path) if (func== "weeklyHistorical") => {
        twitterApi.recentSearchTimeInt(30, path, true)

      }

        // Question 1
      case Array(func, path) if (func == "weekly-pop-emoji") => {
        sparkEmoji.uploadJSON(path, false, false)
        sparkEmoji.topEmojisOfWeek()
      }

      // Question 2
      case Array(func, path, opt) if (func == "popular-emoji-by-time") => {
        sparkEmoji.uploadJSON(path, false, false)
        sparkEmoji.popEmojiByTime(opt)
      }

      //Question 3
      case Array(func, path, lang1, lang2) if(func == "language-top-emojis") =>{
        sparkEmoji.uploadJSON(path, false, false)
        sparkEmoji.langTopEmojisHist(sparkEmoji.dfRaw, lang1, lang2)
      }

      //Question 4
      case Array(func, path, like) if(func == "popular-tweet-emojis") =>{
        sparkEmoji.uploadJSON(path, false, false)
        sparkEmoji.popTweetsEmojiHist(sparkEmoji.dfRaw, like.toLowerCase.toBoolean)
      }

        //Question 5
      case Array(func, path, threshold, seconds) if(func == "popular-people-emojis") =>{
        Future {
          twitterApi.sampleStreamToDir("tweet.fields=public_metrics,created_at,lang&user.fields=public_metrics&expansions=author_id", dirname = path, debug=false)
        }
        sparkEmoji.uploadJSON(path, multiline = false, stream = true)
        sparkEmoji.popPeepsEmojisStream(sparkEmoji.rawDFtoEmojiDFStream(sparkEmoji.dfStreamRaw), threshold.toInt, seconds.toInt)
      }

        //Question 6
      case Array(func, path, emoji, seconds) if(func == "popular-emoji-variant") =>{
        Future {
          twitterApi.sampleStreamToDir("tweet.fields=public_metrics,created_at,lang&user.fields=public_metrics&expansions=author_id", dirname = path, debug=false)
        }
        sparkEmoji.uploadJSON(path, multiline = false, stream = true)
        sparkEmoji.topEmojiVariationStream(sparkEmoji.rawDFtoEmojiDFStream(sparkEmoji.dfStreamRaw), emoji, seconds.toInt)
      }

      // Catch any other cases
      case _ => {
        printMenu()
        System.exit(-1)
      }
    }

  }
  def printMenu(): Unit ={
    println("________________________________________________USAGE_____________________________________________________________")
    println("weeklyHistorical <Dir path>| pull down historical record sample of recent tweets from present time to a week ago")
    println("weekly-pop-emoji <JSON path> | the most popular emoji from the weeklyHistorical data")
    println("popular-emoji-by-time <JSON path> <opt> | the most popular emojis from a set of time periods options: \"daily\" \"hourly\" \"dailyAndHourly\"")
    println("language-top-emojis <JSON path> <first language> <second language> <seconds> | top emojis in first language with how many are used in second language")
    println("popular-tweet-emojis <JSON path> <like boolean> | most liked or retweeted emojis")
    println("popular-people-emojis <JSON Stream path> <followers minimum> <seconds> | most popular emojis among famous people")
    println("popular-emoji-variant <JSON Stream path> <\"base emoji\"> <seconds> | most popular emojis variation of given base emoji")
  }

}
