package org.broadinstitute.dsde.rawls.entities.datarepo

import bio.terra.datarepo.model.SnapshotModel
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.entities.base.ExpressionValidator
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.GatherInputsResult
import org.broadinstitute.dsde.rawls.model.{ErrorReport, MethodConfiguration, ValidatedMethodConfiguration}

import scala.concurrent.{ExecutionContext, Future}
import scala.collection.JavaConverters._

class DataRepoExpressionValidator(override val dataSource: SlickDataSource,
                                  snapshotModel: SnapshotModel)(implicit protected val executionContext: ExecutionContext) extends ExpressionValidator {


  override def validateAndParseMCExpressions(methodConfiguration: MethodConfiguration,
                                             gatherInputsResult: GatherInputsResult,
                                             allowRootEntity: Boolean): Future[ValidatedMethodConfiguration] =
    for {
      validatedMC <- super.validateAndParseMCExpressions(methodConfiguration, gatherInputsResult, allowRootEntity)
      _ <- validateInputRefs(validatedMC)
    } yield validatedMC

  private def validateInputRefs(validatedMethodConfiguration: ValidatedMethodConfiguration): Future[ValidatedMethodConfiguration] = {
    // want to handle cases where there is no attribute string (and should preserve the emptyOptional attributes!)
    val (emptyOptionalInputs, nonEmptyInputs) = validatedMethodConfiguration.validInputs.partition { input =>
      validatedMethodConfiguration.methodConfiguration.inputs(input).value.isEmpty
    }

    // todo: should we validate this value
    val baseTable = validatedMethodConfiguration.methodConfiguration.rootEntityType
      .getOrElse(throw new RawlsExceptionWithErrorReport(ErrorReport("root entity type required for datarepo snapshots")))
      .split('.').last


    val (validInputReferences, invalidInputReferences): (Set[String], Set[String]) = nonEmptyInputs.partition { input =>
      val inputExpression = validatedMethodConfiguration.methodConfiguration.inputs(input).value


      validateReference(baseTable, inputExpression)
    }
    val invalidInputReferencesWithReasons = invalidInputReferences.map(attribute => (attribute, "referenced column does not exist")).toMap

    Future.successful(validatedMethodConfiguration.copy(validInputs = validInputReferences ++ emptyOptionalInputs,
      invalidInputs = validatedMethodConfiguration.invalidInputs ++ invalidInputReferencesWithReasons))
    // then we'd like to take all the validInputs and compare them to TDR snapshot schema somehow...
    // where do we get the schema from? should we be passing it along from higher up or just grabbing it now?
    // how do we do the parsing of the inputs now?

  }

  // todo: idiomaticize this
  private def validateReference(baseTable: String, inputExpression: String): Boolean = {
    // todo: can we pattern match better
    val maybeValid = inputExpression.split('.').toList match {
      case _ :: colName :: Nil => snapshotModel.getTables.asScala.toList.find(_.getName.equals(baseTable)).map { table =>
        table.getColumns.asScala.toList.exists(_.getName.equals(colName))
      }
      case _ => throw new RawlsExceptionWithErrorReport(ErrorReport("we haven't figured this out yet"))
    }

    maybeValid match {
      case Some(_) => _
      case None => false
    }
  }
}
