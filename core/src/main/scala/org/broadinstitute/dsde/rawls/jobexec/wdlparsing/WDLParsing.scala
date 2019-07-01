package org.broadinstitute.dsde.rawls.jobexec.wdlparsing

import cromwell.client.model.WorkflowDescription
import org.broadinstitute.dsde.rawls.model.UserInfo

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try


trait WDLParsing {

  def parse(userInfo: UserInfo, wdl: String)(implicit executionContext: ExecutionContext): Try[WorkflowDescription]
}
