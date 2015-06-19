package org.broadinstitute.dsde.rawls.jobexec

import cromwell.binding.WdlNamespace
import cromwell.binding.types.WdlType
import scala.util.{Success, Failure}

object MethodConfigValidator {
  /**
   * Iterate over the WDL inputs and ensure that (1) all necessary inputs are specified and (2) types are correct.
   * @return map of from WDL input name to error message (so if the map is empty, the config is valid)
   */
  def getValidationErrors(configInputs: Map[String, Any], wdlRaw: String) = {
    WdlNamespace.load(wdlRaw).workflows.head.inputs.map {
      case (wdlName, wdlType) => {
        val errorOption = configInputs.get(wdlName) match {
          case Some(rawValue) => wdlType.coerceRawValue(rawValue) match {
            case Success(wdlValue) => None
            case Failure(exception) => Option("Input is wrong type in method config: " + exception.getMessage)
          }
          case None => Option("Input is missing from method config")
        }
        (wdlName, errorOption)
      }
    } collect { case (name, Some(error)) => (name, error) }
  }
}
