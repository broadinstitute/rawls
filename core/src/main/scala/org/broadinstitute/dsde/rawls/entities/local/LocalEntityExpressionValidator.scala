package org.broadinstitute.dsde.rawls.entities.local

import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.entities.base.ExpressionValidator
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.GatherInputsResult
import org.broadinstitute.dsde.rawls.model.{MethodConfiguration, ValidatedMethodConfiguration}
import slick.dbio.DBIOAction

import scala.concurrent.{ExecutionContext, Future}

class LocalEntityExpressionValidator(protected val dataSource: SlickDataSource)
                                    (implicit protected val executionContext: ExecutionContext)
  extends ExpressionValidator {


  override def validateMCExpressions(methodConfiguration: MethodConfiguration,
                                     gatherInputsResult: GatherInputsResult,
                                     allowRootEntity: Boolean): Future[ValidatedMethodConfiguration] =
    dataSource.inTransaction { parser =>
      DBIOAction.successful {
        internalValidateMCExpressions(methodConfiguration, gatherInputsResult, allowRootEntity, parser.parseMCExpressions)
      }
    }
}
