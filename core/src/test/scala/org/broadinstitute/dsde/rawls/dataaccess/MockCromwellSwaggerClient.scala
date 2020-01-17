package org.broadinstitute.dsde.rawls.dataaccess

import cromwell.client.model.ValueType.TypeNameEnum
import cromwell.client.model.{ToolInputParameter, ToolOutputParameter, ValueType, WorkflowDescription}
import org.broadinstitute.dsde.rawls.model.{UserInfo, WDL}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.util.Try

class MockCromwellSwaggerClient extends CromwellSwaggerClient("fake/path") {

  val workflowDescriptions: mutable.Map[String, WorkflowDescription] =  new TrieMap()

  override def describe(userInfo: UserInfo, wdl: WDL): Try[WorkflowDescription] = {
    Try { MockCromwellSwaggerClient.returnCopy(workflowDescriptions(wdl.source)) }
  }

}

object MockCromwellSwaggerClient {
  def returnCopy(wfDescription: WorkflowDescription): WorkflowDescription = {
    val name = wfDescription.getName

    if (wfDescription.getValid) {
      val inputs = wfDescription.getInputs.asScala.toList.map { input =>
        makeToolInputParameter(input.getName, input.getOptional, input.getValueType, input.getTypeDisplayName)
      }
      val outputs = wfDescription.getOutputs.asScala.toList.map { output =>
        makeToolOutputParameter(output.getName, output.getValueType, output.getTypeDisplayName)
      }
      makeWorkflowDescription(name, inputs, outputs)
    } else makeBadWorkflowDescription(name, wfDescription.getErrors().asScala.toList)
  }

  def makeWorkflowDescription(name: String, inputs: List[ToolInputParameter], outputs: List[ToolOutputParameter]): WorkflowDescription = {
    new WorkflowDescription().name(name).valid(true).inputs(inputs.asJava).outputs(outputs.asJava)
  }

  def makeBadWorkflowDescription(name: String, errors: List[String]): WorkflowDescription = {
    new WorkflowDescription().name(name).valid(false).validWorkflow(false).errors(errors.asJava)
  }

  def makeToolInputParameter(name: String, optional: Boolean, valueType: ValueType, typeDisplayName: String): ToolInputParameter = {
    new ToolInputParameter().name(name).optional(optional).typeDisplayName(typeDisplayName).valueType(valueType)
  }

  def makeToolOutputParameter(name: String, valueType: ValueType, typeDisplayName: String): ToolOutputParameter = {
    new ToolOutputParameter().name(name).typeDisplayName(typeDisplayName).valueType(valueType)
  }


  def makeValueType(value: String): ValueType = {
    new ValueType().typeName(TypeNameEnum.fromValue(value))
  }

  def makeArrayValueType(value: ValueType): ValueType = {
    new ValueType().typeName(TypeNameEnum.ARRAY).arrayType(value)
  }

  def makeOptionalValueType(value: ValueType): ValueType = {
    new ValueType().typeName(TypeNameEnum.OPTIONAL).optionalType(value)
  }
}
