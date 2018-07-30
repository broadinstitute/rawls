package org.broadinstitute.dsde.rawls.jobexec.wdlparsing

import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.ParsedWdlWorkflow

import scala.util.Try

trait WDLParsing {

  def parse(wdl: String): Try[ParsedWdlWorkflow]

}
