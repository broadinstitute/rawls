package org.broadinstitute.dsde.rawls.jobexec.wdlparsing

import java.util.concurrent.TimeUnit

import com.github.benmanes.caffeine.cache.Caffeine
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import cromwell.client.model.WorkflowDescription
import org.broadinstitute.dsde.rawls.dataaccess.CromwellSwaggerClient
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.ParsedWdlWorkflow
import org.broadinstitute.dsde.rawls.jobexec.wdlparsing.CachingThreadPoolWDLParser.{cacheMaxSize, cacheTTLFailure, cacheTTLSuccess, generateCacheKey, generateHash, logger, wdlParsingConf}
import org.broadinstitute.dsde.rawls.model.UserInfo
import scalacache.{Cache, Entry, get, put}
import scalacache.caffeine.CaffeineCache

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Random, Success}
import scala.util.hashing.MurmurHash3

class WDLParser extends WDLParsing with LazyLogging {

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
    .build[String, Entry[Future[WorkflowDescription]]]
  implicit val customisedCaffeineCache: Cache[Future[WorkflowDescription]] = CaffeineCache(underlyingCaffeineCache)

  override def parse(wdl: String): Future[WorkflowDescription] = {
    val tick = System.currentTimeMillis()
    val key = generateCacheKey(wdl)
    val wdlhash = generateHash(wdl)

    logger.info(s"<parseWDL-cache> looking up $wdlhash ...")

    get(key) match {
      case Some(parseResult) =>
        val tock = System.currentTimeMillis() - tick
        logger.info(s"<parseWDL-cache> found cached result for $wdlhash in $tock ms.")
        parseResult
      case None => parseAndCache(wdl, key, tick, wdlhash)
    }
  }

  private def parseAndCache(wdl: String, key: String, tick: Long, wdlhash: String): Future[WorkflowDescription] = {
    logger.info(s"<parseWDL-cache> entering sync block for $wdlhash ...")
    /* Generate the synchronization key. Because synchronization works via object reference equality,
       we need to intern any strings we use. And if we're interning the string, we want it to be small
       to preserve PermGen space.
       Therefore, use an interned hash of the WDL. In the off chance of a hash collision, we end up performing
       unnecessary synchronization, but there should be no other ill effects.
     */
    val syncKey = wdlhash.intern

    syncKey.synchronized {
      get(key) match {
        case Some(parseResult) =>
          val tock = System.currentTimeMillis() - tick
          logger.info(s"<parseWDL-cache> found cached result for $wdlhash in $tock ms.")
          parseResult
        case None => {
          val miss = System.currentTimeMillis() - tick
          logger.info(s"<parseWDL-cache> encountered cache miss for $wdlhash in $miss ms.")

          val parseResult = inContextParse(wdl)

          val parsetime = System.currentTimeMillis() - tick
          logger.info(s"<parseWDL-cache> actively parsed WDL for $wdlhash in $parsetime ms.")

          val ttl = parseResult match {
            case Success(_) => Some(cacheTTLSuccess)
            case Failure(ex) => Some(cacheTTLFailure)
          }

          put(key)(parseResult, ttl = ttl)
          val tock = System.currentTimeMillis() - tick
          logger.info(s"<parseWDL-cache> cached result $wdlhash in $tock ms.")
          parseResult
        }
      }
    }
  }


  private val executionServiceConf = conf.getConfig("executionservice")
  private val readServers = executionServiceConf.getObject("readServers").values()
  private def cromwellClient = new CromwellSwaggerClient(readServers(Random.nextInt(readServers.size())))

  private def inContextParse(userInfo: UserInfo, wdl: String)(implicit executionContext: ExecutionContext): WorkflowDescription = {
    cromwellClient.
  }

  /**
    * generate a short string that identifies this WDL. Should not be used where uniqueness is a strict
    * requirement due to the (low) chance of hash collisions.
    * @param wdl
    * @return
    */
  private def generateHash(wdl: String) = {
    MurmurHash3.stringHash(wdl).toString
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
