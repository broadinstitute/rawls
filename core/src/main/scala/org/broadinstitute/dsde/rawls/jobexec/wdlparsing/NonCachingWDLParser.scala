package org.broadinstitute.dsde.rawls.jobexec.wdlparsing

import cromwell.client.model.WorkflowDescription
import org.broadinstitute.dsde.rawls.config.WDLParserConfig
import org.broadinstitute.dsde.rawls.dataaccess.CromwellSwaggerClient
import org.broadinstitute.dsde.rawls.model.{UserInfo, WDL}

import scala.concurrent.ExecutionContext
import scala.util.Try

class NonCachingWDLParser(wdlParsingConfig: WDLParserConfig, cromwellSwaggerClient: CromwellSwaggerClient)
    extends WDLParser {

  def parse(userInfo: UserInfo, wdl: WDL)(implicit executionContext: ExecutionContext): Try[WorkflowDescription] =
    cromwellSwaggerClient.describe(userInfo, wdl).map { wfDescription =>
      WDLParser.appendWorkflowNameToInputsAndOutputs(wfDescription)
    }

}
