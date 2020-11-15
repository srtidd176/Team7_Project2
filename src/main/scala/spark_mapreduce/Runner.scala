package spark_mapreduce

object Runner {

  val sparkEmoji: SparkEmoji = new SparkEmoji("local[4]")

  def main(args: Array[String]): Unit = {
    if(args.length < 1){
      println("Usage: <function number> <arg1> <arg2> ....")
      System.exit(-1)
    }

    args match {
        // Upload and perform the demoQuery TODO delete this test
      case Array(func, lenMax) => if(func == "demoQuery") {
        sparkEmoji.uploadJSON("people.json",true)
        sparkEmoji.demoQuery(lenMax.toInt)
      }
        // Catch any other cases
      case _ => sparkEmoji.emojiValue("twitter.json")
    }

  }
}
