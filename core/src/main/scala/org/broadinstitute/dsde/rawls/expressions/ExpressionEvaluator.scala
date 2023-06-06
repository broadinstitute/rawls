package org.broadinstitute.dsde.rawls.expressions

import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, EntityRecord, ReadWriteAction}
import org.broadinstitute.dsde.rawls.entities.base.ExpressionEvaluationSupport.{EntityName, LookupExpression}
import org.broadinstitute.dsde.rawls.entities.base.InputExpressionReassembler
import org.broadinstitute.dsde.rawls.expressions.parser.antlr._
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.MethodInput
import org.broadinstitute.dsde.rawls.model.{AttributeValue, Workspace}

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

object ExpressionEvaluator {
  def withNewExpressionEvaluator[R](dataAccess: DataAccess, rootEntities: Option[Seq[EntityRecord]])(
    op: ExpressionEvaluator => ReadWriteAction[R]
  )(implicit executionContext: ExecutionContext): ReadWriteAction[R] =
    SlickExpressionEvaluator.withNewExpressionEvaluator(dataAccess, rootEntities) { slickEvaluator =>
      op(new ExpressionEvaluator(slickEvaluator, slickEvaluator.rootEntities))
    }

  def withNewExpressionEvaluator[R](dataAccess: DataAccess,
                                    workspaceContext: Workspace,
                                    rootType: String,
                                    rootName: String
  )(op: ExpressionEvaluator => ReadWriteAction[R])(implicit executionContext: ExecutionContext): ReadWriteAction[R] =
    SlickExpressionEvaluator.withNewExpressionEvaluator(dataAccess, workspaceContext, rootType, rootName) {
      slickEvaluator =>
        op(new ExpressionEvaluator(slickEvaluator, slickEvaluator.rootEntities))
    }
}

class ExpressionEvaluator(slickEvaluator: SlickExpressionEvaluator, val rootEntities: Option[Seq[EntityRecord]]) {

  /**
  The overall approach is:
        - Parse the input expression using ANTLR Extended JSON parser
        - Visit the parsed tree to find all the look up nodes (i.e. attribute reference expressions)
        - If there are no look up nodes, evaluate the input expression using the JSONEvaluator
        - If there are look up nodes:
            - for each look up node, evaluate the attribute reference through SlickEvaluator
            - through a series of transformations, generate a Map of entity name to Map of lookup expressions and their
              evaluated value for that entity
            - for each entity, substitute the evaluated values of attribute references back into the input expression
              by visiting the parsed tree of input expression
            - for each entity, pass the reconstructed input expression to JSONEvaluator to parse the expression into
              AttributeValue
    To help understand the approach if their are attribute references present, we will follow the below example roughly:
      expression = "{"exampleRef1":this.bam, "exampleIndex":this.index}"
      rootEntities = Seq(101, 102) (here we assume the entity name is 101 for Entity Record 1 and 102 for Entity record 2)

    The final output will be:
    ReadWriteAction(Map(
          "101" -> Try(Seq(AttributeValueRawJson("{"exampleRef1":"gs://abc", "exampleIndex":123}"))),
          "102" -> Try(Seq(AttributeValueRawJson("{"exampleRef1":"gs://def", "exampleIndex":456}")))
        )
    )
    */
  def evalFinalAttribute(workspaceContext: Workspace, expression: String, input: Option[MethodInput] = None)(implicit
    executionContext: ExecutionContext
  ): ReadWriteAction[Map[EntityName, Try[Iterable[AttributeValue]]]] = {

    // parse expression using ANTLR TerraExpression parser
    val terraExpressionParser = AntlrTerraExpressionParser.getParser(expression)
    val localFinalAttributeEvaluationVisitor = new LocalEvaluateToAttributeVisitor(workspaceContext, slickEvaluator)
      with WorkspaceLookups
      with LocalEntityLookups

    Try(terraExpressionParser.root()) match {
      case Success(parsedTree) =>
        /*
          Evaluate all attribute reference expressions if any.
          For our example:
            lookupNodes = Set("this.bam", "this.index")
         */

        localFinalAttributeEvaluationVisitor.visit(parsedTree).map { result =>
          InputExpressionReassembler
            .constructFinalInputValues(result, parsedTree, rootEntities.map(_.map(_.name)), input)
        }

      case Failure(regrets) => slickEvaluator.dataAccess.driver.api.DBIO.failed(regrets)
    }
  }

  def evalWorkspaceExpressionsOnly(workspaceContext: Workspace, expression: String)(implicit
    executionContext: ExecutionContext
  ): ReadWriteAction[Map[LookupExpression, Try[Iterable[AttributeValue]]]] = {
    val extendedJsonParser = AntlrTerraExpressionParser.getParser(expression)
    val localFinalAttributeEvaluationVisitor = new LocalEvaluateToAttributeVisitor(workspaceContext, slickEvaluator)
      with WorkspaceLookups

    Try(extendedJsonParser.root()) match {
      case Success(parsedTree) =>
        localFinalAttributeEvaluationVisitor.visit(parsedTree).map { result =>
          // workspace expression results do not vary by entity so just grab the head result
          // ideally this is called with rootEntities None anyway
          result.map { case (lookupExpression, valueByEntity) => lookupExpression -> valueByEntity.toSeq.head._2 }.toMap
        }

      case Failure(regrets) => slickEvaluator.dataAccess.driver.api.DBIO.failed(regrets)
    }
  }

  def evalFinalEntity(workspaceContext: Workspace, expression: String)(implicit
    executionContext: ExecutionContext
  ): ReadWriteAction[Iterable[EntityRecord]] = {
    val terraExpressionParser = AntlrTerraExpressionParser.getParser(expression)
    val localFinalEntityEvaluationVisitor = new LocalEvaluateToEntityVisitor(workspaceContext, slickEvaluator)

    Try(terraExpressionParser.root()) match {
      case Success(parsedTree) => localFinalEntityEvaluationVisitor.visit(parsedTree)
      case Failure(regrets)    => slickEvaluator.dataAccess.driver.api.DBIO.failed(regrets)
    }
  }
}
