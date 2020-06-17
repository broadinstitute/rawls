package org.broadinstitute.dsde.rawls.expressions.parser.antlr

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.ExtendedJSONParser.{ArrContext, LiteralContext, ObjContext, RelationContext}

import scala.util.{Failure, Try}

/** Output expressions don't allow for entity references (relations) in the middle or for any JSON. They must
  * be of the form this.attribute or workspace.attribute */
class LocalOutputExpressionValidationVisitor(allowRootEntity: Boolean)
  extends LocalInputExpressionValidationVisitor(allowRootEntity = allowRootEntity) {

  override def visitRelation(ctx: RelationContext): Try[Unit] = {
    Failure(new RawlsException("Entity references not permitted in the middle of output expressions"))
  }

  override def visitObj(ctx: ObjContext): Try[Unit] = {
    Failure(new RawlsException("Output expressions cannot be JSON"))
  }

  override def visitArr(ctx: ArrContext): Try[Unit] = {
    Failure(new RawlsException("Output expressions cannot be JSON"))
  }

  override def visitLiteral(ctx: LiteralContext): Try[Unit] = {
    Failure(new RawlsException("Output expressions cannot be JSON"))
  }
}
