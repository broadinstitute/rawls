package org.broadinstitute.dsde.rawls.jobexec.wdlparsing

import com.github.benmanes.caffeine.cache.Caffeine
import com.typesafe.scalalogging.LazyLogging
import cromwell.client.model.WorkflowDescription
import org.broadinstitute.dsde.rawls.config.WDLParserConfig
import org.broadinstitute.dsde.rawls.dataaccess.CromwellSwaggerClient
import org.broadinstitute.dsde.rawls.model.UserInfo
import scalacache.{Cache, Entry, get, put}
import scalacache.caffeine.CaffeineCache

import scala.concurrent.ExecutionContext
import scala.util.hashing.MurmurHash3
import scala.util.{Failure, Success, Try}

class CachingWDLParser(wdlParsingConfig: WDLParserConfig, cromwellSwaggerClient: CromwellSwaggerClient) extends WDLParser with LazyLogging {

  // set up cache for WDL parsing
  /* from scalacache doc: "Note: If you’re using an in-memory cache (e.g. Guava or Caffeine) then it makes sense
     to use the synchronous mode. But if you’re communicating with a cache over a network (e.g. Redis, Memcached)
     then this mode is not recommended. If the network goes down, your app could hang forever!"
   */
  import scalacache.modes.sync._

  private val underlyingCaffeineCache = Caffeine.newBuilder()
    .maximumSize(wdlParsingConfig.cacheMaxSize)
    .build[String, Entry[Try[WorkflowDescription]]]
  implicit val customisedCaffeineCache: Cache[Try[WorkflowDescription]] = CaffeineCache(underlyingCaffeineCache)


  override def parse(userInfo: UserInfo, wdl: String)(implicit executionContext: ExecutionContext): Try[WorkflowDescription] = {
    val tick = System.currentTimeMillis()
    val key = generateCacheKey(wdl)
    //wdlhash is for logging purposes as we don't want to log full wdls
    val wdlHash = generateHash(wdl)

    logger.info(s"<parseWDL-cache> looking up $wdlHash ...")
    get(key) match {
      case Some(parseResult) =>
        val tock = System.currentTimeMillis() - tick
        logger.info(s"<parseWDL-cache> found cached result for $wdlHash in $tock ms.")
        parseResult
      case None =>
        val miss = System.currentTimeMillis() - tick
        logger.info(s"<parseWDL-cache> encountered cache miss for $wdlHash in $miss ms.")
        parseAndCache(userInfo, wdl, key, wdlHash, tick)
    }
  }

  private def parseAndCache(userInfo: UserInfo, wdl: String, key: String, wdlHash: String, tick: Long)(implicit executionContext: ExecutionContext): Try[WorkflowDescription] = {
    val parseResult: Try[WorkflowDescription] = inContextParse(userInfo, wdl) map { wfDescription =>
      WDLParser.appendWorkflowNameToInputsAndOutputs(wfDescription)
    }

    val parsetime = System.currentTimeMillis() - tick
    logger.info(s"<parseWDL-cache> actively parsed WDL for $wdlHash in $parsetime ms.")

    val timeToLive = parseResult match {
      case Success(_) => Some(wdlParsingConfig.cacheTTLSuccessSeconds)
      case Failure(ex) =>
        logger.debug(s"<parseWDL-cache> parse failed with with exception $ex on WDL $wdlHash")
        Some(wdlParsingConfig.cacheTTLFailureSeconds)
    }

    put(key)(parseResult, ttl = timeToLive)

    val tock = System.currentTimeMillis() - tick
    logger.info(s"<parseWDL-cache> cached result $wdlHash in $tock ms.")
    parseResult
  }



  private def inContextParse(userInfo: UserInfo, wdl: String)(implicit executionContext: ExecutionContext): Try[WorkflowDescription] = {
   cromwellSwaggerClient.describe(userInfo, wdl)
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
