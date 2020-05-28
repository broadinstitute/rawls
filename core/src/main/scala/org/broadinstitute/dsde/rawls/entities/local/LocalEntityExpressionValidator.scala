package org.broadinstitute.dsde.rawls.entities.local

import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.entities.base.ExpressionValidator

import scala.concurrent.ExecutionContext

class LocalEntityExpressionValidator(val dataSource: SlickDataSource)(implicit protected val executionContext: ExecutionContext) extends ExpressionValidator
