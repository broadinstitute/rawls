package org.broadinstitute.dsde.rawls.entities.datarepo

import bio.terra.datarepo.model.SnapshotModel
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.entities.base.ExpressionValidator
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.GatherInputsResult
import org.broadinstitute.dsde.rawls.model.{ErrorReport, MethodConfiguration, ValidatedMethodConfiguration}

import scala.concurrent.Future
import scala.collection.JavaConverters._

class DataRepoExpressionValidator(protected val parser: DataRepoExpressionParser,
                                  snapshotModel: SnapshotModel) extends ExpressionValidator {

  override def validateMCExpressions(methodConfiguration: MethodConfiguration,
                                     gatherInputsResult: GatherInputsResult,
                                     allowRootEntity: Boolean): Future[ValidatedMethodConfiguration] = {
    val validatedMC = internalValidateMCExpressions(methodConfiguration, gatherInputsResult, allowRootEntity, parser.parseMCExpressions)
    validateInputRefs(validatedMC)
  }

  private def validateInputRefs(validatedMethodConfiguration: ValidatedMethodConfiguration): Future[ValidatedMethodConfiguration] = {
    // want to handle cases where there is no attribute string (and should preserve the emptyOptional attributes!)
    val (emptyOptionalInputs, nonEmptyInputs) = validatedMethodConfiguration.validInputs.partition { input =>
      validatedMethodConfiguration.methodConfiguration.inputs(input).value.isEmpty
    }

    val baseTable = validatedMethodConfiguration.methodConfiguration.rootEntityType
      .getOrElse(throw new RawlsExceptionWithErrorReport(ErrorReport("root entity type required for datarepo snapshots")))

    val (validInputReferences, invalidInputReferences): (Set[String], Set[String]) = nonEmptyInputs.partition { input =>
      val inputExpression = validatedMethodConfiguration.methodConfiguration.inputs(input).value

      validateReference(baseTable, parseExpression(inputExpression))
    }
    val invalidInputReferencesWithReasons = invalidInputReferences.map(attribute => (attribute, "referenced column does not exist")).toMap

    Future.successful(validatedMethodConfiguration.copy(validInputs = validInputReferences ++ emptyOptionalInputs,
      invalidInputs = validatedMethodConfiguration.invalidInputs ++ invalidInputReferencesWithReasons))
  }

  private def parseExpression(inputExpression: String): List[String] = inputExpression.split('.').toList

  // todo: idiomaticize this
  private def validateReference(baseTable: String, expressionSteps: List[String]): Boolean = {
    // todo: can we pattern match better
    expressionSteps match {
      case _ :: colName :: Nil => snapshotModel.getTables.asScala.toList.find(_.getName.equals(baseTable)).fold(false) { table =>
        table.getColumns.asScala.toList.exists(_.getName.equals(colName))
      }
      // this is speculation bc it is dependent on TDR client returning relationship info
//      case _ :: relationshipName :: remainingRefs => snapshotModel.getTables.asScala.toList.find(_.getName.equals(baseTable)).map { table =>
//        table.getRelationships.asScala.toList.exists(_.getName.equals(relationshipName)) && validateReference(relationship.to.table, remainingRefs)
//      }
      case _ => throw new RawlsExceptionWithErrorReport(ErrorReport("Invalid expression"))
    }
  }
}
