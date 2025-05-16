package org.broadinstitute.dsde.rawls.expressions.parser.antlr

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.expressions.OutputExpression
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.TerraExpressionParser.EntityLookupContext
import org.broadinstitute.dsde.rawls.model.{Attribute, ErrorReport}

import scala.util.{Failure, Try}

/** Data Repo output entity expressions are not currently supported */
class DataRepoOutputExpressionValidationVisitor extends OutputExpressionValidationVisitor {

  override def visitEntityLookup(ctx: EntityLookupContext): Try[Attribute => OutputExpression] =
    Failure(
      new RawlsExceptionWithErrorReport(
        ErrorReport(
          StatusCodes.BadRequest,
          "Output expressions beginning with \"this.\" are not currently supported when using Data Repo. However, workspace attributes can be used."
        )
      )
    )
}
