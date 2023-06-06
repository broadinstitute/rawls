package org.broadinstitute.dsde.rawls.jobexec.wdlparsing

import cromwell.client.model.WorkflowDescription
import org.broadinstitute.dsde.rawls.model.{UserInfo, WDL}

import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._
import scala.util.Try

trait WDLParser {

  def parse(userInfo: UserInfo, wdl: WDL)(implicit executionContext: ExecutionContext): Try[WorkflowDescription]

}

object WDLParser {

  def appendWorkflowNameToInputsAndOutputs(workflowDescription: WorkflowDescription): WorkflowDescription = {
    val wfName = workflowDescription.getName
    val wfdescriptionInputs = workflowDescription.getInputs.asScala.toList.map { input =>
      input.name(s"$wfName.${input.getName}")
    }
    val wfdescriptionOutputs = workflowDescription.getOutputs.asScala.toList map { output =>
      output.name(s"$wfName.${output.getName}")
    }
    workflowDescription.inputs(wfdescriptionInputs.asJava).outputs(wfdescriptionOutputs.asJava)
  }
}
