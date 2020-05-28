package org.broadinstitute.dsde.rawls.entities.base

import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.GatherInputsResult
import org.broadinstitute.dsde.rawls.model.{MethodConfiguration, ValidatedMethodConfiguration}

import scala.concurrent.Future
import scala.util.Try

trait ExpressionValidator {
  /** validate a MC, skipping optional empty inputs, and return a ValidatedMethodConfiguration */
  def validateAndParseMCExpressions(methodConfiguration: MethodConfiguration,
                                    gatherInputsResult: GatherInputsResult,
                                    allowRootEntity: Boolean): Future[ValidatedMethodConfiguration]

  /** validate a MC, skipping optional empty inputs, and return failure when any inputs/outputs are invalid */
  def validateExpressionsForSubmission(methodConfiguration: MethodConfiguration,
                                       gatherInputsResult: GatherInputsResult,
                                       allowRootEntity: Boolean): Future[Try[ValidatedMethodConfiguration]]
}
