package org.broadinstitute.dsde.rawls.expressions.parser.antlr

import bio.terra.datarepo.model.SnapshotModel
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.TerraExpressionParser.EntityLookupContext

import scala.util.{Failure, Success, Try}

/** rootEntityType is required to deal with TDR, but it's passed in as an option because enforcing this is a question
  * of validation which is the job of this ValidationVisitor. In other words, all validation rules should be
  * implemented in the ValidationVisitors */
class DataRepoInputExpressionValidationVisitor(allowRootEntity: Boolean,
                                               rootEntityType: Option[String],
                                               snapshotModel: SnapshotModel)
  extends TerraExpressionBaseVisitor[Try[Unit]] {

  override def defaultResult() = Success(())

  override def aggregateResult(aggregate: Try[Unit], nextResult: Try[Unit]): Try[Unit] = aggregate.flatMap(_ => nextResult)

  // Entity lookup nodes are only allowed if allowRootEntity is true
  override def visitEntityLookup(ctx: EntityLookupContext): Try[Unit] = {
    if (allowRootEntity) {
      /** TODO: grab the base table name within rootEntityType, then compare that table name, the relations and the
        * attribute name to the snapshotModel */
    } else {
      Failure(new RawlsException("Expressions beginning with \"this.\" are only allowed when running with workspace data model. However, workspace attributes can be used."))
    }
  }

}
