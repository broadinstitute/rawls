package org.broadinstitute.dsde.rawls.dataaccess

import cromwell.client.model.ValueType.TypeNameEnum
import cromwell.client.model._
import org.broadinstitute.dsde.rawls.model.{UserInfo, WDL}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Try

class MockCromwellSwaggerClient extends CromwellSwaggerClient("fake/path") {

  val workflowDescriptions: mutable.Map[WDL, WorkflowDescription] = new TrieMap()

  override def describe(userInfo: UserInfo, wdl: WDL): Try[WorkflowDescription] = {
    if (!workflowDescriptions.contains(wdl)) {
      throw new Exception(
        "Danger! Possible misconfigured test: the MockCromwellSwaggerClient received a request for a WDL it doesn't have and will return an error. " +
          "This may cause a test that asserts a bad request to pass for the wrong reason." +
          s">>> Target WDL is $wdl" +
          s">>> Available WDLs are ${workflowDescriptions.keys.mkString("\n")}"
      )
    }
    Try(MockCromwellSwaggerClient.returnCopy(workflowDescriptions(wdl)))
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

  def makeWorkflowDescription(name: String,
                              inputs: List[ToolInputParameter],
                              outputs: List[ToolOutputParameter]
  ): WorkflowDescription =
    new WorkflowDescription().name(name).valid(true).inputs(inputs.asJava).outputs(outputs.asJava)

  def makeBadWorkflowDescription(name: String, errors: List[String]): WorkflowDescription =
    new WorkflowDescription().name(name).valid(false).validWorkflow(false).errors(errors.asJava)

  def makeToolInputParameter(name: String,
                             optional: Boolean,
                             valueType: ValueType,
                             typeDisplayName: String
  ): ToolInputParameter =
    new ToolInputParameter().name(name).optional(optional).typeDisplayName(typeDisplayName).valueType(valueType)

  def makeToolOutputParameter(name: String, valueType: ValueType, typeDisplayName: String): ToolOutputParameter =
    new ToolOutputParameter().name(name).typeDisplayName(typeDisplayName).valueType(valueType)

  def makeValueType(value: String): ValueType =
    new ValueType().typeName(TypeNameEnum.fromValue(value))

  def makeArrayValueType(value: ValueType): ValueType =
    new ValueType().typeName(TypeNameEnum.ARRAY).arrayType(value)

  def makeOptionalValueType(value: ValueType): ValueType =
    new ValueType().typeName(TypeNameEnum.OPTIONAL).optionalType(value)

  def makeObjectValueType(fields: Map[String, ValueType]): ValueType = {
    val objectFields = fields.map { case (name, valueType) =>
      new ValueTypeObjectFieldTypes().fieldName(name).fieldType(valueType)
    }
    val objectValueType = new ValueType().typeName(TypeNameEnum.OBJECT)
    objectValueType.setObjectFieldTypes(objectFields.toList.asJava)
    objectValueType
  }
}
