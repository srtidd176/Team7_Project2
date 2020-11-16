package spark_mapreduce

object Runner {

  val sparkEmoji: SparkEmoji = new SparkEmoji("local[4]")


  def main(args: Array[String]): Unit = {
    args match {
        //Returns a DataFrame containing the emojis separated from historic Twitter data
      case Array(func, path) if(func == "historic-emojis") =>  {
        sparkEmoji.uploadJSON(path, true, false)
        sparkEmoji.emojiValue(sparkEmoji.dfRaw)
      }
        //Returns a DataFrame containing the emojis separated from Twitter Stream data
      case Array(func, path, seconds) if(func == "stream-emojis") => {
        sparkEmoji.uploadJSON(path, true, true)
        sparkEmoji.emojiValue(sparkEmoji.dfStreamRaw).writeStream.outputMode("complete")
          .format("console")
          .start()
          .awaitTermination(seconds.toInt * 1000)
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
    println("historic-emojis <JSON path> | returns a DataFrame containing the emojis separated from historic Twitter data ")
    println("stream-emojis <JSON path> <seconds> | returns a DataFrame containing the emojis separated from Twitter Stream data")
  }

}
