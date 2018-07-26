package org.broadinstitute.dsde.rawls.jobexec

import java.util.concurrent.{Callable, ExecutorService, Executors, TimeUnit}

import languages.wdl.draft2.WdlDraft2LanguageFactory.httpResolver
import org.broadinstitute.dsde.rawls.RawlsException
import wdl.draft2.model.{WdlNamespaceWithWorkflow, WdlWorkflow}
import wdl.draft2.parser.WdlParser.SyntaxError

import scala.util.{Failure, Try}
import scala.collection.JavaConverters._


object ThreadPoolWDLParser {

  // TODO: move thread count and timeouts to conf
  val executorService: ExecutorService = Executors.newFixedThreadPool(8)

  def parse(wdl: String): Try[WdlWorkflow] = {
    try {
      threadedParse(wdl)
    } catch {
      case e: Exception => Failure(e)
    }
  }

  private def threadedParse(wdl: String): Try[WdlWorkflow] =
    executorService.invokeAny(List(new WDLParser(wdl)).asJava, 45, TimeUnit.SECONDS)

}


class WDLParser(wdl: String) extends Callable[Try[WdlWorkflow]] {
  override def call(): Try[WdlWorkflow] = {
    val parsed: Try[WdlNamespaceWithWorkflow] = WdlNamespaceWithWorkflow.load(wdl, Seq(httpResolver(_))).recoverWith { case t: SyntaxError =>
      Failure(new RawlsException("Failed to parse WDL: " + t.getMessage()))
    }
    parsed map( _.workflow )
  }
}
