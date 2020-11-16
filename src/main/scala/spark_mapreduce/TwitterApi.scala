package spark_mapreduce

import java.io.{BufferedReader, File, InputStreamReader, PrintWriter}
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

  def sampleStreamToDir(fieldQuery:String ="", dirname:String="twitterstream", linesPerFile:Int=1000, debug:Boolean) = {
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





  /*
     * This method calls the filtered stream endpoint and streams Tweets from it
     * *//*
     * This method calls the filtered stream endpoint and streams Tweets from it
     * */
   def filterStream(fieldQuery: String="", dirname:String="twitterFilterStream", linesPerFile:Int=50, debug:Boolean): Unit = {
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
   def recentSearch (searchQuery: String, queryFields: String = "") = {
    var searchResponse: String = ""

    val httpClient = HttpClients.custom.setDefaultRequestConfig(RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build).build
    val uriBuilder = new URIBuilder(s"https://api.twitter.com/2/tweets/search/recent?${queryFields}")

     val queryParameters = new util.ArrayList[NameValuePair]
     queryParameters.add(new BasicNameValuePair("query", searchQuery))
     uriBuilder.addParameters(queryParameters)
    println(uriBuilder.toString() )

    val httpGet = new HttpGet(uriBuilder.build)
    httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken))
    httpGet.setHeader("Content-Type", "application/json")
    val response = httpClient.execute(httpGet)
    val entity = response.getEntity
    if (null != entity) searchResponse = EntityUtils.toString(entity, "UTF-8")
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

  /* Helper method to setup rules before streaming data */
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

}
