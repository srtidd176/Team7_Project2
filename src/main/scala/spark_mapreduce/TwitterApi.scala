package spark_mapreduce

import java.io.{BufferedReader, File, FileWriter, InputStreamReader, PrintWriter}
import java.nio.file.{Files, Paths}
import java.util

import scala.collection.mutable.{ArrayBuffer, Map}
import org.apache.http.client.config.{CookieSpecs, RequestConfig}
import org.apache.http.{HttpEntity, NameValuePair}
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.client.utils.URIBuilder
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.message.BasicNameValuePair
import org.apache.http.util.EntityUtils
import twitter4j.{JSONArray, JSONObject}
import java.time.{OffsetDateTime, ZoneOffset, ZonedDateTime}


class TwitterApi (bearerToken: String)  {

  /**
   * Opens up an Https connection to the Twitter v2 api route for a sample stream.
   * @param fieldQuery :String (default: "") The query request made to the twitter Api
   *              reference for available fields - https://developer.twitter
   *              .com/en/docs/twitter-api/tweets/sampled-stream/api-reference/get-tweets-sample-stream
   * @param dirname : String (default: "twitterstream") - Specifies  a name for the directory that will store
   *                incoming Tweet data.
   * @param linesPerFile :Int - number of lines data that are saved to File
   * */

  def sampleStreamToDir(fieldQuery:String ="", dirname:String="twitterstream", linesPerFile:Int=1000, debug:Boolean):Unit = {
    val httpClient = HttpClients.custom.setDefaultRequestConfig(RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build).build
    val uriBuilder = new URIBuilder(s"https://api.twitter.com/2/tweets/sample/stream?${fieldQuery}")
    val httpGet = new HttpGet(uriBuilder.build)
    httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken))
    val response = httpClient.execute(httpGet)
    val entity = response.getEntity
    new File(Paths.get(s"${dirname}/").toUri ).mkdir()
    if (null != entity) {
      fileIO(entity, dirname, linesPerFile, debug)
    }
  }


  /**
   * filterStream connects you to a filteredStream of Twitter Data; it is required that you have set up
   * your rules previously with setupRules(Map[String,String])
   *
   * @param fieldQuery :String (default: "") The query request made to the twitter Api
   *                         reference for returned fields https://developer.twitter
   *                         .com/en/docs/twitter-api/tweets/filtered-stream/api-reference
   * @param dirname : String (default: "twitterFilterStream") - Specifies  a name for the directory that will store
   *                         incoming Tweet data.
   * @param linesPerFile :Int - number of lines data that are saved to File
   */
   def filterStream(fieldQuery: String="", dirname:String="twitterFilterStream", linesPerFile:Int=50, debug: Boolean = true): Unit = {
    val httpClient = HttpClients.custom.setDefaultRequestConfig(RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build).build
    val uriBuilder = new URIBuilder(s"https://api.twitter.com/2/tweets/search/stream?${fieldQuery}")
    val httpGet = new HttpGet(uriBuilder.build)
    httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken))
    val response = httpClient.execute(httpGet)
    val entity = response.getEntity
    new File(Paths.get(s"${dirname}/").toUri ).mkdir()
    if (null != entity) {
      fileIO(entity, dirname, linesPerFile, debug)
    }
  }


  /**
   *   Connect to the Twitter API on the recent search Route. Limited to a historical record of a 7-days,
   *   limited at 100 days
   * @param searchQuery - Required search query
   * @param queryFields - optional fields query
   * @return String of requested data.
   */
  def recentSearch (searchQuery: String, queryFields: String = "", dirname:String="recentSearch", debug: Boolean = false): String = {
    var searchResponse: String = ""

    val httpClient = HttpClients.custom.setDefaultRequestConfig(RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build).build
    val uriBuilder = new URIBuilder(s"https://api.twitter.com/2/tweets/search/recent?${queryFields}")

    val queryParameters = new util.ArrayList[NameValuePair]
    queryParameters.add(new BasicNameValuePair("query", searchQuery))
    uriBuilder.addParameters(queryParameters)

    if (debug) println(uriBuilder.toString() )

    val httpGet = new HttpGet(uriBuilder.build)
    httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken))
    httpGet.setHeader("Content-Type", "application/json")
    val response = httpClient.execute(httpGet)

    val entity = response.getEntity
    new File(Paths.get(s"${dirname}/").toUri ).mkdir()

    if (null != entity) {
      searchResponse = EntityUtils.toString(entity, "UTF-8")
      if (debug) println(searchResponse)
      val timestamp = new JSONObject(searchResponse).get("data").asInstanceOf[JSONArray].get(0).asInstanceOf[JSONObject].get("created_at").toString.filter(_ !=
        ':')
      val nextToken = new JSONObject(searchResponse).getJSONObject("meta").get("next_token").toString
      val fileWriter = new FileWriter(new File(s"${dirname}/time-${timestamp}-${nextToken}"))
      fileWriter.write(searchResponse)
      fileWriter.close()
    }
    searchResponse
  }

  /**
   * Helper function to read from a HttpEntity response, store files in a directory, and set linesPerFile
   * @param entity - a response.getEntity from our HTTPS request
   * @param dirname - name of a directory where our responses will be stored
   * @param linesPerFile - lines each stored file should possess
   */
  private def fileIO (entity: HttpEntity, dirname: String, linesPerFile: Int, debug: Boolean): Unit = {
    val reader = new BufferedReader(new InputStreamReader(entity.getContent))
    var line = reader.readLine
    //initial filewriter, will be replaced with new filewriter every linesperfile
    var fileWriter = new PrintWriter(Paths.get("tweetstream.tmp").toFile)
    var lineNumber = 1 //track line number to know when to move to new file
    val millis = System.currentTimeMillis() //identify this job with millis
    while ( {
      line != null
    }) {
      if(lineNumber % linesPerFile == 0) {
        fileWriter.close()
        Files.move(
          Paths.get("tweetstream.tmp"),
          Paths.get(s"${dirname}/tweetstream-${millis}-${lineNumber/linesPerFile}"))
        fileWriter = new PrintWriter(Paths.get("tweetstream.tmp").toFile)
      }
      fileWriter.println(line)
      if(debug){println(line)}
      line = reader.readLine()
      lineNumber += 1
    }
  }

  /**
   * Method to setup Rules to the Filter Stream route
   * @param rules : scala.collection.mutable.Map[String, String] of rules.
   *              (https://developer.twitter.com/en/docs/twitter-api/tweets/filtered-stream/integrate/build-a-rule)
   *              example rules: Map("cats has:images" -> "cat images")
   */
   def setupRules( rules: Map[String, String]): Unit = {

    val existingRules = getRules()
    if (existingRules.size > 0) deleteRules( existingRules)
    createRules(rules)
  }

  /* Helper method to create rules for filtering */
  private def createRules( rules: Map[String, String]): Unit = {
    val httpClient = HttpClients.custom.setDefaultRequestConfig(RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build).build
    val uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/search/stream/rules")
    val httpPost = new HttpPost(uriBuilder.build)
    httpPost.setHeader("Authorization", String.format("Bearer %s", bearerToken))
    httpPost.setHeader("content-type", "application/json")
    val body = new StringEntity(getFormattedString("{\"add\": [%s]}", rules))
    httpPost.setEntity(body)
    val response = httpClient.execute(httpPost)
    val entity = response.getEntity
    if (null != entity) System.out.println(EntityUtils.toString(entity, "UTF-8"))
  }

  /* Helper method to get existing rule */
  private def getRules() = {
    val rules = ArrayBuffer[String]()

    val httpClient = HttpClients.custom.setDefaultRequestConfig(RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build).build
    val uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/search/stream/rules")
    val httpGet = new HttpGet(uriBuilder.build)
    httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken))
    httpGet.setHeader("content-type", "application/json")
    val response = httpClient.execute(httpGet)
    val entity = response.getEntity

    if (null != entity) {
      val json = new JSONObject(EntityUtils.toString(entity, "UTF-8"))
      if (json.length > 1) {
        val array = json.get("data").asInstanceOf[JSONArray]
        var i = 0
        while ( {
          i < array.length
        }) {
          val jsonObject = array.get(i).asInstanceOf[JSONObject]
          rules += (jsonObject.getString("id"))

          i += 1
        }
      }
    }
    rules
  }

  /* Helper method to delete rules */
  private def deleteRules( existingRules: ArrayBuffer[String]): Unit = {
    val httpClient = HttpClients.custom.setDefaultRequestConfig(RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build).build
    val uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/search/stream/rules")
    val httpPost = new HttpPost(uriBuilder.build)
    httpPost.setHeader("Authorization", String.format("Bearer %s", bearerToken))
    httpPost.setHeader("content-type", "application/json")
    val body = new StringEntity(getFormattedString("{ \"delete\": { \"ids\": [%s]}}", existingRules))
    httpPost.setEntity(body)
    val response = httpClient.execute(httpPost)
    val entity = response.getEntity
    if (null != entity) System.out.println(EntityUtils.toString(entity, "UTF-8"))
  }

  private def getFormattedString(string: String, ids: ArrayBuffer[String]): String = {
    val sb = new StringBuilder
    if (ids.size == 1) String.format(string, "\"" + ids(0) + "\"")
    else {

      for (id <- ids) {
        sb.append("\"" + id + "\"" + ",")
      }
      val result = sb.toString
      String.format(string, result.substring(0, result.length - 1))
    }
  }

  private def getFormattedString(string: String, rules: Map[String, String]): String = {
    val sb = new StringBuilder
    if (rules.size == 1) {
      val key = rules.keySet.iterator.next
      String.format(string, "{\"value\": \"" + key + "\", \"tag\": \"" + rules.get(key) + "\"}")
    }
    else {
      for ((value, tag) <- rules) {
        //        val value = entry.getKey
        //        val tag = entry.getValue
        sb.append("{\"value\": \"" + value + "\", \"tag\": \"" + tag + "\"}" + ",")
      }
      val result = sb.toString
      String.format(string, result.substring(0, result.length - 1))
    }
  }

  /**
   * recentSearchTimeInt makes calls to the TwitterApi for recentSearch (currently predifined search Query + queryFields) from the present moment to 7 days
   * ago and saves the results to a file in recentSearch. Returns a max of 100 Tweets per request.
   * @param interval timeinterval to divide the ZonedDateTimes from present to 7 days ago by.
   */
  def recentSearchTimeInt(interval: Int, path:String = "recentSearch", debug: Boolean = false): Unit = {
    System.setProperty("https.protocols", "TLSv1,TLSv1.1,TLSv1.2")
    val searchQuery = "\uD83D\uDE00 OR \uD83D\uDE03 OR \uD83D\uDE04 OR \uD83D\uDE01 OR  \uD83D\uDE06 OR  \uD83D\uDE05 OR \uD83D\uDE02 OR" +
      " \uD83E\uDD23 OR ☺️ OR \uD83D\uDE0A OR \uD83D\uDE07 OR \uD83D\uDE42 OR \uD83D\uDE43 OR \uD83D\uDE09 OR \uD83D\uDE0C OR \uD83D\uDE0D OR \uD83E\uDD70 OR \uD83D\uDE18 OR \uD83D\uDE17 OR \uD83D\uDE19 OR \uD83D\uDE1A OR \uD83D\uDE0B OR \uD83D\uDE1B OR \uD83D\uDE1D OR \uD83D\uDE1C OR \uD83E\uDD2A OR \uD83E\uDD28 OR \uD83E\uDDD0 OR \uD83E\uDD13 OR \uD83D\uDE0E OR \uD83E\uDD29 OR \uD83E\uDD73 OR \uD83D\uDE0F OR \uD83D\uDE12 OR \uD83D\uDE1E OR \uD83D\uDE14 OR \uD83D\uDE1F OR \uD83D\uDE15 OR \uD83D\uDE41 OR ☹️ OR \uD83D\uDE23 OR \uD83D\uDE16 OR \uD83D\uDE2B OR \uD83D\uDE29 OR \uD83E\uDD7A OR \uD83D\uDE22 OR \uD83D\uDE2D OR \uD83D\uDE24 OR \uD83D\uDE20 OR \uD83D\uDE21 OR \uD83E\uDD2C OR \uD83E\uDD2F OR \uD83D\uDE33 OR \uD83E\uDD75 OR \uD83E\uDD76 OR \uD83D\uDE31 OR \uD83D\uDE28 OR \uD83D\uDE30 OR \uD83D\uDE25 OR \uD83D\uDE13 OR \uD83E\uDD17 OR \uD83E\uDD14 OR \uD83E\uDD2D OR \uD83E\uDD2B OR \uD83E\uDD25 OR \uD83D\uDE36 OR \uD83D\uDE10 OR \uD83D\uDE11 OR \uD83D\uDE2C OR \uD83D\uDE44 OR \uD83D\uDE2F OR \uD83D\uDE26 OR \uD83D\uDE27 OR  \uD83D\uDE2E OR \uD83D\uDE32 OR \uD83E\uDD71 OR \uD83D\uDE34 OR \uD83E\uDD24 OR \uD83D\uDE2A OR \uD83D\uDE35 OR \uD83E\uDD10 OR \uD83E\uDD74 OR \uD83E\uDD22 OR \uD83E\uDD2E OR \uD83E\uDD27"
    val queryFields = "max_results=100&tweet.fields=public_metrics,created_at,lang&user.fields=public_metrics&expansions=author_id"
    val dateVectors = dateIntervalVector(interval)

    for( dateTime <- dateVectors) {
      if (debug) println("Value of dateTime: " + dateTime);
      recentSearch(searchQuery, s"end_time=${dateTime.toString}&" + queryFields, path , debug=debug)
    }
  }

  /**
   * Helper function which accepts an interval of time for a timeWindow then returns a Vector of ZonedDateTimes from the present moment in UTC to 7 days prior
   * @param timeWindow - timeinterval to divide the ZonedDateTimes from present to 7 days ago by.
   * @return Vector[ZonedDateTime]
   */
  private def dateIntervalVector (timeWindow: Int ): Vector[ZonedDateTime] = {
    val now = OffsetDateTime.now(ZoneOffset.UTC).toZonedDateTime
    val weekAgo = now.minusDays(7).plusMinutes(timeWindow)
    var window = now.minusMinutes(timeWindow)
    var dateVectors: Vector[ZonedDateTime] = Vector(now.minusSeconds(10))

    while (weekAgo.isBefore(window)) {
      window = window.minusMinutes(timeWindow)
      dateVectors = dateVectors :+ window
    }
    dateVectors
  }



}
