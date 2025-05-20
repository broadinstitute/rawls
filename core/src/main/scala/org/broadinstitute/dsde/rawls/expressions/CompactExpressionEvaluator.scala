package org.broadinstitute.dsde.rawls.expressions

import org.broadinstitute.dsde.rawls.dataaccess.slick.ReadAction
import org.broadinstitute.dsde.rawls.entities.compact.CompactEntityRepository
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.{AntlrTerraExpressionParser, CompactEvaluateVisitor}
import org.broadinstitute.dsde.rawls.expressions.parser.antlr.CompactEvaluateVisitor.AttributeLookup
import org.broadinstitute.dsde.rawls.model.AttributeValue
import slick.dbio.DBIO

import java.util.UUID
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

class CompactExpressionEvaluator(repository: CompactEntityRepository) {

  def evaluateExpression(workspaceId: UUID,
                         expression: String,
                         entityType: String,
                         entityName: String
  ): Future[Seq[AttributeValue]] = {
    val lookup = parseLookups(expression)

    repository.dataSource.inTransaction { _ =>
      lookUpToQuery(workspaceId, lookup, entityType, entityName)
    }
  }

  def parseLookups(expression: String): Seq[AttributeLookup] = {
    val terraExpressionParser = AntlrTerraExpressionParser.getParser(expression)
    val visitor = new CompactEvaluateVisitor()
    Try(terraExpressionParser.root()) match {
      case Success(parsedTree) =>
        visitor.visit(parsedTree).map { result =>
          // result is Seq[AttributeLookup]
          result
        }
      case Failure(_) => Seq.empty
    }
  }
  // one for root, one for relations, one for cases.  do not scale per entity

  // TODO multiple lookups
  def lookUpToQuery(workspaceId: UUID,
                    lookups: Seq[AttributeLookup],
                    entityType: String,
                    entityName: String
  ): ReadAction[Seq[AttributeValue]] = {
    // if there is one lookup with no relations
    DBIO.sequence(lookups.map { lookup =>
      repository.queries.queryEntityForAttribute(workspaceId, entityType, entityName, lookups(0).attributeName)
    })
    // if there is one lookup with relations
//    repository.queries.queryRelationsForAttribute(workspaceId,
//                                                  lookups(0).relations(0).getText,
//                                                  lookups(0).attributeName,
//                                                  entityType,
//                                                  entityName
//    )

    /*
        is different for workspace attributes
        should be generalized to multiple lookups (or lookups with multiple entries)
     */
  }

}
