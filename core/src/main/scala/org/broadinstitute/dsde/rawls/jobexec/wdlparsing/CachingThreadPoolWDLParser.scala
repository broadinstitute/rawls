package org.broadinstitute.dsde.rawls.jobexec.wdlparsing

import java.util.concurrent.{Callable, ExecutorService, Executors, TimeUnit}

import com.github.benmanes.caffeine.cache.Caffeine
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import languages.wdl.draft2.WdlDraft2LanguageFactory.httpResolver
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.ParsedWdlWorkflow
import org.broadinstitute.dsde.rawls.model.MethodOutput
import scalacache.{Cache, Entry, get, put}
import scalacache.caffeine.CaffeineCache
import wdl.draft2.model.WdlNamespaceWithWorkflow
import wdl.draft2.parser.WdlParser.SyntaxError

import scala.collection.JavaConverters._
import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.util.hashing.MurmurHash3
import scala.util.{Failure, Success, Try}


object CachingThreadPoolWDLParser extends WDLParsing with LazyLogging {

  // TODO: conf should be injected, not read directly. At least this is an object so it happens once.
  private val conf = ConfigFactory.parseResources("version.conf").withFallback(ConfigFactory.load())
  private val wdlParsingConf = conf.getConfig("wdl-parsing")

  private val cacheMaxSize = wdlParsingConf.getInt("cache-max-size")
  private val cacheTTLSuccess = Duration(wdlParsingConf.getInt("cache-ttl-success-seconds"), TimeUnit.SECONDS)
  private val cacheTTLFailure = Duration(wdlParsingConf.getInt("cache-ttl-failure-seconds"), TimeUnit.SECONDS)
  private val threadPoolTimeout = wdlParsingConf.getInt("parser-thread-pool-timeout-seconds")

  // set up cache for WDL parsing
  /* from scalacache doc: "Note: If you’re using an in-memory cache (e.g. Guava or Caffeine) then it makes sense
     to use the synchronous mode. But if you’re communicating with a cache over a network (e.g. Redis, Memcached)
     then this mode is not recommended. If the network goes down, your app could hang forever!"
   */
  import scalacache.modes.sync._
  private val underlyingCaffeineCache = Caffeine.newBuilder()
    .maximumSize(cacheMaxSize)
    .build[String, Entry[Try[ParsedWdlWorkflow]]]
  implicit val customisedCaffeineCache: Cache[Try[ParsedWdlWorkflow]] = CaffeineCache(underlyingCaffeineCache)

  // set up thread pool
  val executorService: ExecutorService = Executors.newCachedThreadPool()

  def parse(wdl: String): Try[ParsedWdlWorkflow] = {
    val tick = System.currentTimeMillis()
    val key = generateCacheKey(wdl)
    logger.info(s"<parseWDL-cache> looking up $key ...")
    key.synchronized {
      get(key) match {
        case Some(parseResult) =>
          val tock = System.currentTimeMillis() - tick
          logger.info(s"<parseWDL-cache> found cached result for $key in $tock ms.")
          parseResult
        case None =>
          val miss = System.currentTimeMillis() - tick
          logger.info(s"<parseWDL-cache> encountered cache miss for $key in $miss ms.")
          val parseResult = threadedParse(wdl)
          val parsetime = System.currentTimeMillis() - tick
          logger.info(s"<parseWDL-cache> actively parsed WDL for $key in $parsetime ms.")
          val ttl = parseResult match {
            case Success(_) => Some(cacheTTLSuccess)
            case Failure(ex) => ex.getCause match {
              case se:SyntaxError =>
                // syntax error is an expected, deterministic response to invalid wdl. cache this equivalent to a success.
                Some(cacheTTLSuccess)
              case _  =>
                // other errors may be transient, such as timeouts retrieving http imports, or timeouts
                // on the thread pool because parsing was slow
                Some(cacheTTLFailure)
            }
          }
          put(key)(parseResult, ttl = ttl)
          val tock = System.currentTimeMillis() - tick
          logger.info(s"<parseWDL-cache> cached result $key in $tock ms.")
          parseResult
      }
    }
  }

  // an alternate terse/idiomatic implementation, but can't get good logging
  /*
  def parse(wdl: String): Try[ParsedWdlWorkflow] = {
    sync.caching(generateCacheKey(wdl))(ttl = None) {
      threadedParse(wdl)
    }
  }
  */

  private def threadedParse(wdl: String): Try[ParsedWdlWorkflow] = {
    try {
      blocking {
        executorService.invokeAny(List(new CallableParser(wdl)).asJava, threadPoolTimeout, TimeUnit.SECONDS)
      }
    } catch {
      case e: Exception => Failure(e)
    }
  }

  private def generateCacheKey(wdl: String): String = {
    // reduce key size to keep cache small. No attachment to the Murmur algorithm specifically.
    MurmurHash3.stringHash(wdl).toString.intern()
  }

}


class CallableParser(wdl: String) extends Callable[Try[ParsedWdlWorkflow]] {
  override def call(): Try[ParsedWdlWorkflow] = {
    val parsed: Try[WdlNamespaceWithWorkflow] = WdlNamespaceWithWorkflow.load(wdl, Seq(httpResolver(_))).recoverWith { case t: SyntaxError =>
      Failure(new RawlsException("Failed to parse WDL: " + t.getMessage()))
    }

    parsed map { p => ParsedWdlWorkflow(p.workflow.inputs, p.workflow.outputs.map(o => MethodOutput(o.locallyQualifiedName(p.workflow), o.womType.toDisplayString))) }
  }
}
