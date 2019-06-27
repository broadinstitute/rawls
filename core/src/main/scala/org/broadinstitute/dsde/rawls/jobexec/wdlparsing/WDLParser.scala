package org.broadinstitute.dsde.rawls.jobexec.wdlparsing

import java.util.concurrent.TimeUnit

import com.github.benmanes.caffeine.cache.Caffeine
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import cromwell.client.model.WorkflowDescription
import org.broadinstitute.dsde.rawls.dataaccess.CromwellSwaggerClient
import org.broadinstitute.dsde.rawls.model.UserInfo
import scalacache.{Cache, Entry, get, put}
import scalacache.caffeine.CaffeineCache

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Random, Success, Try}
import scala.collection.JavaConverters._

class WDLParser(cromwellSwaggerClient: CromwellSwaggerClient) extends WDLParsing with LazyLogging {

  // TODO: conf should be injected, not read directly. At least this is an object so it happens once.
  private val conf = ConfigFactory.parseResources("version.conf").withFallback(ConfigFactory.load())
  private val wdlParsingConf = conf.getConfig("wdl-parsing")
  private val cacheMaxSize = wdlParsingConf.getInt("cache-max-size")
  private val cacheTTLSuccess = Duration(wdlParsingConf.getInt("cache-ttl-success-seconds"), TimeUnit.SECONDS)
  private val cacheTTLFailure = Duration(wdlParsingConf.getInt("cache-ttl-failure-seconds"), TimeUnit.SECONDS)

  // set up cache for WDL parsing
  /* from scalacache doc: "Note: If you’re using an in-memory cache (e.g. Guava or Caffeine) then it makes sense
     to use the synchronous mode. But if you’re communicating with a cache over a network (e.g. Redis, Memcached)
     then this mode is not recommended. If the network goes down, your app could hang forever!"
   */
  import scalacache.modes.sync._

  private val underlyingCaffeineCache = Caffeine.newBuilder()
    .maximumSize(cacheMaxSize)
    .build[String, Entry[Try[WorkflowDescription]]]
  implicit val customisedCaffeineCache: Cache[Try[WorkflowDescription]] = CaffeineCache(underlyingCaffeineCache)


  override def parse(userInfo: UserInfo, wdl: String)(implicit executionContext: ExecutionContext): Try[WorkflowDescription] = {
    val tick = System.currentTimeMillis()
    val key = generateCacheKey(wdl)

    get(key) match {
      case Some(parseResult) =>
        val tock = System.currentTimeMillis() - tick
        parseResult
      case None => parseAndCache(userInfo, wdl, key)
    }
  }

  private def parseAndCache(userInfo: UserInfo, wdl: String, key: String)(implicit executionContext: ExecutionContext): Try[WorkflowDescription] = {
    val parseResult = inContextParse(userInfo, wdl)
    val timeToLive = parseResult match {
      case Success(_) => Some(cacheTTLSuccess)
      case Failure(ex) => Some(cacheTTLFailure)
    }
    put(key)(parseResult, ttl = timeToLive)
    parseResult
  }



  private def inContextParse(userInfo: UserInfo, wdl: String)(implicit executionContext: ExecutionContext): Try[WorkflowDescription] = {
   Try { cromwellSwaggerClient.validate(userInfo, wdl) }
  }

  /**
    * this method exists as an abstraction, making it easy to change what we use as a cache key in case
    * we want to reduce large wdl payloads to something smaller. Current implementation returns the wdl
    * unchanged to ensure correctness of cache lookups.
    *
    * @param wdl
    * @return
    */
  private def generateCacheKey(wdl: String): String = {
    wdl
  }

}
