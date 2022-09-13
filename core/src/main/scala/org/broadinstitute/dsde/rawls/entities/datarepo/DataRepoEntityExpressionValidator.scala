package org.broadinstitute.dsde.rawls.entities.datarepo

import bio.terra.datarepo.model.SnapshotModel
import org.broadinstitute.dsde.rawls.entities.base.ExpressionValidator
import org.broadinstitute.dsde.rawls.expressions.OutputExpression
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.{
  AntlrTerraExpressionParser,
  DataRepoInputExpressionValidationVisitor,
  DataRepoOutputExpressionValidationVisitor
}

import scala.util.Try

class DataRepoEntityExpressionValidator(snapshotModel: SnapshotModel) extends ExpressionValidator {
  override protected[entities] def validateInputExpr(
    rootEntityTypeOption: Option[String]
  )(expression: String): Try[Unit] = {
    val terraExpressionParser = AntlrTerraExpressionParser.getParser(expression)
    val visitor = new DataRepoInputExpressionValidationVisitor(rootEntityTypeOption, snapshotModel)

    /*
      parse the expression using ANTLR parser for local input expressions and walk the tree using `visit()` to examine
      child nodes. If it finds an entityLookup node at any point, it fails unless allowRootEntity is true since
      entity expressions are only allowed when running with the workspace data model
     */
    Try(terraExpressionParser.root()).flatMap(visitor.visit)
  }

  override protected[entities] def validateOutputExpr(
    rootEntityTypeOption: Option[String]
  )(expression: String): Try[Unit] = {
    val terraExpressionParser = AntlrTerraExpressionParser.getParser(expression)
    val visitor = new DataRepoOutputExpressionValidationVisitor

    for {
      parseTree <- Try(terraExpressionParser.root())
      _ <- visitor.visit(parseTree)
      _ <- OutputExpression.validate(expression, rootEntityTypeOption)
    } yield ()
  }
}
