package org.broadinstitute.dsde.rawls.jobexec
import cromwell.client.model.{ToolInputParameter, WorkflowDescription}
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.{GatherInputsResult, MethodInput}
import org.broadinstitute.dsde.rawls.jobexec.wdlparsing.WDLParser
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.{model, RawlsException}
import spray.json._

import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._
import scala.language.postfixOps
import scala.util.Try

class MethodConfigResolver(wdlParser: WDLParser) {
  def parseWDL(userInfo: UserInfo, wdl: WDL)(implicit executionContext: ExecutionContext): Try[WorkflowDescription] =
    wdlParser.parse(userInfo, wdl)

  def gatherInputs(userInfo: UserInfo, methodConfig: MethodConfiguration, wdl: WDL)(implicit
    executionContext: ExecutionContext
  ): Try[GatherInputsResult] = parseWDL(userInfo, wdl) map { parsedWdlWorkflow =>
    def isAttributeEmpty(fqn: String): Boolean =
      methodConfig.inputs.get(fqn) match {
        case Some(AttributeString(value)) => value.isEmpty
        case _ =>
          throw new RawlsException(
            s"MethodConfiguration ${methodConfig.namespace}/${methodConfig.name} input ${fqn} value is unavailable"
          )
      }

    val wdlInputs = parsedWdlWorkflow.getInputs.asScala

    /* ok, so there are:
        - inputs that the WDL requires and the MC defines: ok - definedInputs
        - inputs that the WDL says are optional but the MC defines anyway: ok - contained in definedInputs
        - inputs that the WDL requires and the MC does not define: bad - missingRequired
        - inputs that the WDL says are optional and the MC doesn't define: ok - emptyOptionals
        - inputs that the WDL has no idea about but the MC defines anyway: bad - extraInputs
     */
    val (definedInputs, undefinedInputs) = wdlInputs.partition { input =>
      methodConfig.inputs.contains(input.getName) && !isAttributeEmpty(input.getName)
    }
    val (emptyOptionals, missingRequired) = undefinedInputs.partition(input => input.getOptional)
    val extraInputs = methodConfig.inputs.filterNot { case (name, expression) =>
      wdlInputs.map(_.getName).contains(name)
    }

    GatherInputsResult(
      definedInputs.map(input => MethodInput(input, methodConfig.inputs(input.getName).value)).toSet,
      emptyOptionals.map { input =>
        MethodInput(input, methodConfig.inputs.getOrElse(input.getName, AttributeString("")).value)
      }.toSet,
      missingRequired.map(_.getName).toSet,
      extraInputs.keys.toSet
    )
  }

  /**
    * Convert result of resolveInputs to WDL input format, ignoring AttributeNulls.
    * @return serialized JSON to send to Cromwell
    */
  def propertiesToWdlInputs(inputs: Map[String, Attribute]): String = JsObject(
    inputs flatMap {
      case (key, AttributeNull) => None
      case (key, notNullValue)  => Some(key, notNullValue.toJson(WDLJsonSupport.attributeFormat))
    }
  ) toString

  def getMethodInputsOutputs(userInfo: UserInfo, wdl: WDL)(implicit
    executionContext: ExecutionContext
  ): Try[MethodInputsOutputs] = parseWDL(userInfo, wdl) map { workflowDescription =>
    if (workflowDescription.getValid) {
      val inputs = workflowDescription.getInputs.asScala.toList map { input =>
        model.MethodInput(input.getName, input.getTypeDisplayName.replaceAll("\\n", ""), input.getOptional)
      }
      val outputs = workflowDescription.getOutputs.asScala.toList map { output =>
        model.MethodOutput(output.getName, output.getTypeDisplayName.replace("\\n", ""))
      }
      MethodInputsOutputs(inputs, outputs)
    } else throw new RawlsException(workflowDescription.getErrors.asScala.mkString("\n"))
  }

  def toMethodConfiguration(userInfo: UserInfo, wdl: WDL, methodRepoMethod: MethodRepoMethod)(implicit
    executionContext: ExecutionContext
  ): Try[MethodConfiguration] = {
    val empty = AttributeString("")
    getMethodInputsOutputs(userInfo, wdl) map { io =>
      val inputs = io.inputs map { _.name -> empty }
      val outputs = io.outputs map { _.name -> empty }
      MethodConfiguration("namespace",
                          "name",
                          Some("rootEntityType"),
                          Some(Map.empty[String, AttributeString]),
                          inputs.toMap,
                          outputs.toMap,
                          methodRepoMethod
      )
    }
  }

}

object MethodConfigResolver {
  case class MethodInput(workflowInput: ToolInputParameter, expression: String)

  case class GatherInputsResult(processableInputs: Set[MethodInput],
                                emptyOptionalInputs: Set[MethodInput],
                                missingInputs: Set[String],
                                extraInputs: Set[String]
  )

}
