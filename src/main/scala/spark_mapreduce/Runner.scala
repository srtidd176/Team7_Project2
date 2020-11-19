package spark_mapreduce

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object Runner {

  val sparkEmoji: SparkEmoji = new SparkEmoji("local[4]")
  val twitterApi: TwitterApi = new TwitterApi(System.getenv("TWITTER_BEARER_TOKEN"))

  def main(args: Array[String]): Unit = {
    args match {
        //TODO delete : Returns a DataFrame containing the emojis separated from historic Twitter data
      case Array(func, path) if(func == "historic-emojis") =>  {
        sparkEmoji.uploadJSON(path, multiline = false, stream = false)
        sparkEmoji.emojiValue(sparkEmoji.dfRaw).show()
      }
        //TODO delete : Returns a DataFrame containing the emojis separated from Twitter Stream data
      case Array(func, path, seconds) if(func == "stream-emojis") => {
        Future {
          twitterApi.sampleStreamToDir("tweet.fields=public_metrics,created_at,lang&user.fields=public_metrics&expansions=author_id",debug=false)
        }
        sparkEmoji.uploadJSON(path, multiline=true, stream=true)
        sparkEmoji.emojiValueStream(sparkEmoji.dfStreamRaw).select("id", "text").writeStream
          .outputMode("append")
          .format("console")
          .start()
          .awaitTermination(seconds.toInt*1000)
      }


        //Question 5
      case Array(func, path, threshold, seconds) if(func == "popular-people-emojis") =>{
        Future {
          twitterApi.sampleStreamToDir("tweet.fields=public_metrics,created_at,lang&user.fields=public_metrics&expansions=author_id",debug=false)
        }
        sparkEmoji.uploadJSON(path, multiline = false, stream = true)
        sparkEmoji.popPeepsEmojisStream(sparkEmoji.emojiValueStream(sparkEmoji.dfStreamRaw), threshold.toInt, seconds.toInt)
      }

        //Question 6
      case Array(func, path, emoji, seconds) if(func == "popular-emoji-variant") =>{
        Future {
          twitterApi.sampleStreamToDir("tweet.fields=public_metrics,created_at,lang&user.fields=public_metrics&expansions=author_id",debug=false)
        }
        sparkEmoji.uploadJSON(path, multiline = false, stream = true)
        sparkEmoji.topEmojiVariationStream(sparkEmoji.emojiValueStream(sparkEmoji.dfStreamRaw), emoji, seconds.toInt)
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
    println("historic-emojis <JSON path> | emojis info separated from historic Twitter data ")
    println("stream-emojis <JSON path> <seconds> | emojis info separated from Twitter Stream data")
    println("popular-people-emojis <JSON path> <followers minimum> <seconds> | most popular emojis among famous people")
    println("popular-emoji-variant <JSON path> <base emoji encoding> <seconds> | most popular emojis variation of given base emoji")
  }

}
