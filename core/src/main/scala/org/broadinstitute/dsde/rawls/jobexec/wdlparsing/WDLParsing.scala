package org.broadinstitute.dsde.rawls.jobexec.wdlparsing

import wdl.draft2.model.WdlWorkflow

import scala.util.Try

trait WDLParsing {

  def parse(wdl: String): Try[WdlWorkflow]

}
