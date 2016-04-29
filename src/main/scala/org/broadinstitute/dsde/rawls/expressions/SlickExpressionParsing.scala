package org.broadinstitute.dsde.rawls.expressions

import java.nio.ByteBuffer
import java.util.UUID

import _root_.slick.dbio
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.model._
import scala.concurrent.ExecutionContext
import scala.util.{Try, Failure, Success}
import scala.util.parsing.combinator.JavaTokenParsers

case class SlickExpressionContext(workspaceContext: SlickWorkspaceContext, rootType: String, rootName: String)

trait SlickExpressionParser extends JavaTokenParsers {
  this: DriverComponent with EntityComponent with WorkspaceComponent with AttributeComponent =>
  import driver.api._

  // A parsed expression will result in a PipelineQuery. Each step in a query traverses from entity to
  // entity following references. The final step takes the last entity, producing a result dependent on the query
  // (e.g. loading the entity itself, getting an attribute). The root step produces an entity to start the pipeline.
  // If rootStep is None, steps should also be empty and finalStep does all the work (e.g. literals).
  case class PipelineQuery(rootStep: Option[RootFunc], steps: List[PipeFunc], finalStep: FinalFunc)

  // starting with just an expression context produces a PipeResult
  type RootFunc = (SlickExpressionContext) => PipeType

  // extends the input PipeType producing a PipeType that traverses further down the chain
  type PipeFunc = (SlickExpressionContext, PipeType) => PipeType

  // converts the incoming PipeType into the appropriate db action
  // PipeType may be None when there is no pipeline (e.g. literals, workspace attributes)
  type FinalFunc = (SlickExpressionContext, Option[PipeType]) => ReadAction[Iterable[Any]]

  // a query that results in entity records and a sort ordering
  type PipeType = Query[EntityTable, EntityRecord, Seq]

  /** Parser definitions **/
  // Entity expressions take the general form entity.ref.ref.attribute.
  // For now, we expect the initial entity to be the special token "this", which is bound at evaluation time to a root entity.

  //Parser for expressions ending in a value (not a reference)
  private def attributeExpression: Parser[PipelineQuery] = {
    // the basic case: this.(ref.)*attribute
    entityRootDot ~ rep(entityRefDot) ~ valueAttribute ^^ {
      case root ~ Nil ~ last => PipelineQuery(Option(root), List.empty, last)
      case root ~ ref ~ last => PipelineQuery(Option(root), ref, last)
    } |
    // attributes at the end of a reference chain starting at a workspace: workspace.ref.(ref.)*attribute
    workspaceEntityRefDot ~ rep(entityRefDot) ~ valueAttribute ^^ {
      case workspace ~ Nil ~ last => PipelineQuery(Option(workspace), List.empty, last)
      case workspace ~ ref ~ last => PipelineQuery(Option(workspace), ref, last)
    } |
    // attributes directly on a workspace: workspace.attribute
    workspaceAttribute ^^ {
      case workspace => PipelineQuery(None, List.empty, workspace)
    } |
    //Literal parsing. These should be removed and folded into complex expressions once we support them.
    literalNum ^^ {
      case num => PipelineQuery(None, List(), num)
    } |
    literalString ^^ {
      case str => PipelineQuery(None, List(), str)
    }
  }

  //Parser for output expressions: this.attribute or workspace.attribute (no entity references in the middle)
  private def outputExpression: Parser[PipelineQuery] = {
    entityRootDot ~ valueAttribute ^^ {
      case root ~ attr => PipelineQuery(Option(root), List.empty, attr)
    } |
    workspaceAttribute ^^ {
      case workspace => PipelineQuery(None, List.empty, workspace)
    }
  }

  //Parser for expressions ending in an attribute that's a reference to another entity
  private def entityExpression: Parser[PipelineQuery] = {
    // reference IS the entity: this
    entityRoot ^^ {
      case root => PipelineQuery(Option(root), List.empty, entityFinalFunc)
    } |
    // reference chain starting with an entity: this.(ref.)*ref
    entityRootDot ~ rep(entityRefDot) ~ entityRef ^^ {
      case root ~ Nil ~ last => PipelineQuery(Option(root), List(last), entityFinalFunc)
      case root ~ ref ~ last => PipelineQuery(Option(root), ref :+ last, entityFinalFunc)
    } |
    // reference chain starting with the workspace: workspace.(ref.)*ref
    workspaceEntityRefDot ~ rep(entityRefDot) ~ entityRef ^^ {
      case workspace ~ Nil ~ last => PipelineQuery(Option(workspace), List(last), entityFinalFunc)
      case workspace ~ ref ~ last => PipelineQuery(Option(workspace), ref :+ last, entityFinalFunc)
    } |
    // reference directly off the workspace: workspace.ref
    workspaceEntityRef ^^ {
      case workspace => PipelineQuery(None, List.empty, workspace)
    }
  }

  // just root by itself with no refs or attributes
  private def entityRoot: Parser[RootFunc] =
    "this$".r ^^ { _ => entityRootFunc}

  // root followed by dot meaning it is to be followed by refs or attributes
  private def entityRootDot: Parser[RootFunc] =
    "this." ^^ { _ => entityRootFunc}

  // workspace.attribute, note that this is a FinalFunc - because workspaces are not entities they can be piped the same way
  private def workspaceAttribute: Parser[FinalFunc] =
    "workspace." ~> ident ^^ {
      case name if Attributable.reservedAttributeNames.contains(name) => workspaceReservedAttributeFinalFunc(name)
      case name => workspaceAttributeFinalFunc(name)
    }

  // an entity reference as the final attribute in an expression
  private def entityRef: Parser[PipeFunc] =
    ident ^^ { case entity => entityNameAttributePipeFunc(entity)}

  // an entity reference in the middle of an expression
  private def entityRefDot: Parser[PipeFunc] =
    ident <~ "." ^^ { case name => entityNameAttributePipeFunc(name)}

  // an entity reference after the workspace, this can be a RootFunc because it resolves to an entity query that can be piped
  private def workspaceEntityRefDot: Parser[RootFunc] =
    "workspace." ~> ident <~ "." ^^ { case name => workspaceEntityRefRootFunc(name)}

  // an entity reference after the workspace, note that this is a FinalFunc - because workspaces are not entities they can be piped the same way
  private def workspaceEntityRef: Parser[FinalFunc] =
    "workspace." ~> ident ^^ { case name => workspaceEntityFinalFunc(name)}

  // the last attribute has no dot after it
  private def valueAttribute: Parser[FinalFunc] =
    ident ^^ {
      case name if Attributable.reservedAttributeNames.contains(name) => entityReservedAttributeFinalFunc(name)
      case name => entityAttributeFinalFunc(name)
    }

  private def literalString: Parser[FinalFunc] =
    """^\".*\"$""".r ^^ { case str => stringFunc(str) }

  private def literalNum: Parser[FinalFunc] =
    floatingPointNumber ^^ { case num => floatFunc(num) }

  def parseAttributeExpr(expression: String) = {
    parse(expression, attributeExpression)
  }

  def parseOutputExpr(expression: String) = {
    parse(expression, outputExpression)
  }

  def parseEntityExpr(expression: String) = {
    parse(expression, entityExpression)
  }

  private def parse(expression: String, parser: Parser[PipelineQuery] ) = {
    parseAll(parser, expression) match {
      case Success(result, _) => scala.util.Success(result)
      case NoSuccess(msg, next) => scala.util.Failure(new RuntimeException("Failed at line %s, column %s: %s".format(next.pos.line, next.pos.column, msg)))
    }
  }

  /** functions against pipes **/

  // the root function starts the pipeline at some root entity type in the workspace
  private def entityRootFunc(context: SlickExpressionContext): PipeType = {
    for {
      entity <- entityQuery.findEntityByName(context.workspaceContext.workspaceId, context.rootType, context.rootName)
    } yield entity
  }

  // final func that gets an attribute off a workspace
  private def workspaceAttributeFinalFunc(attributeName: String)(context: SlickExpressionContext, shouldBeNone: Option[PipeType]): ReadAction[Iterable[Attribute]] = {
    assert(shouldBeNone.isEmpty)

    val wsIdAndAttributeQuery = for {
      workspace <- workspaceQuery.findByIdQuery(context.workspaceContext.workspaceId)
      attribute <- workspaceAttributeQuery if attribute.ownerId === workspace.id && attribute.name === attributeName
    } yield (workspace.id, attribute)

    wsIdAndAttributeQuery.result.map { wsIdAndAttributes =>
      // the query restricts all results to have workspace id === context.workspaceContext.workspaceId
      // unmarshalAttributes requires a structure of ((ws id, attribute rec), option[entity rec]) where
      // the optional entity rec is used for references. Since we know we are not dealing with a reference here
      // as this is the attribute final func, we can pass in None.
      val attributesOption = workspaceAttributeQuery.unmarshalAttributes(wsIdAndAttributes.map((_, None))).get(context.workspaceContext.workspaceId)
      attributesOption.map { attributes => Seq(attributes.getOrElse(attributeName, AttributeNull)) }.getOrElse(Seq.empty)
    }
  }

  // root func that gets an entity reference off a workspace
  private def workspaceEntityRefRootFunc(entityRefName: String)(context: SlickExpressionContext): PipeType = {
    for {
      workspace <- workspaceQuery.findByIdQuery(context.workspaceContext.workspaceId)
      attribute <- workspaceAttributeQuery if attribute.ownerId === workspace.id && attribute.name === entityRefName
      nextEntity <- entityQuery if attribute.valueEntityRef === nextEntity.id
    } yield nextEntity
  }

  // add pipe to an entity referenced by the current entity
  private def entityNameAttributePipeFunc(entityRefName: String)(context: SlickExpressionContext, queryPipeline: PipeType): PipeType = {
    for {
      entity <- queryPipeline
      attribute <- entityAttributeQuery if entity.id === attribute.ownerId && attribute.name === entityRefName
      nextEntity <- entityQuery if attribute.valueEntityRef === nextEntity.id
    } yield nextEntity
  }

  // filter attributes to only the given attributeName and convert to attribute
  private def entityAttributeFinalFunc(attributeName: String)(context: SlickExpressionContext, queryPipeline: Option[PipeType]): ReadAction[Iterable[Attribute]] = {
    // attributeForNameQuery will only contain attribute records of the given name but for possibly more than 1 entity
    // and in the case of a list there will be more than one attribute record for an entity
    val attributeForNameQuery = (for {
      entity <- queryPipeline.get
      attribute <- entityAttributeQuery if entity.id === attribute.ownerId && attribute.name === attributeName
    } yield (entity.name, attribute)).result


    attributeForNameQuery.map { entityWithAttributeRecs =>
      // unmarshalAttributes requires a structure of ((entity id, attribute rec), option[entity rec]) where
      // the optional entity rec is used for references. Since we know we are not dealing with a reference here
      // as this is the attribute final func, we can pass in None.
      val attributesByEntityId = entityAttributeQuery.unmarshalAttributes(entityWithAttributeRecs.map { case (entityId, attrRec) => ((entityId, attrRec), None) })
      val namedAttributesOnlyByEntityId = attributesByEntityId.mapValues(_.getOrElse(attributeName, AttributeNull)).toSeq

      // need to sort here because some of the manipulations above don't preserve order so we can't sort in the query
      val orderedEntityNameAndAttributes = namedAttributesOnlyByEntityId.sortWith { case ((entityName1, _), (entityName2, _)) =>
        entityName1 < entityName2
      }

      orderedEntityNameAndAttributes.map { case (_, attribute) => attribute }
    }
  }

  // final func that handles reserved attributes on an entity
  private def entityReservedAttributeFinalFunc(attributeName: String)(context: SlickExpressionContext, queryPipeline: Option[PipeType]): ReadAction[Iterable[Attribute]] = {
    val baseQuery = queryPipeline.get join entityQuery on (_.id === _.id)
    attributeName match {
      case "name" => baseQuery.sortBy(_._1.name).distinct.result.map(_.map(rec => AttributeString(rec._2.name)))
      case "entityType" => baseQuery.sortBy(_._1.name).distinct.result.map(_.map(x => AttributeString(x._2.entityType)))
    }
  }

  // final func that handles reserved attributes on a workspace
  private def workspaceReservedAttributeFinalFunc(attributeName: String)(context: SlickExpressionContext, shouldBeNone: Option[PipeType]): ReadAction[Iterable[Attribute]] = {
    assert(shouldBeNone.isEmpty)

    attributeName match {
      case "name" => DBIO.successful(Seq(AttributeString(context.workspaceContext.workspace.name)))
      case "entityType" => throw new RawlsException("entityType not valid for workspace")
    }
  }

  //Takes a list of entities at the end of a pipeline and returns them in final format.
  private def entityFinalFunc(context: SlickExpressionContext, queryPipeline: Option[PipeType]): ReadAction[Iterable[Entity]] = {
    val query: entityQuery.EntityQuery = queryPipeline.get
    val entitiesAction = entityQuery.unmarshalEntities(entityQuery.joinOnAttributesAndRefs(query))
    entitiesAction.zip(queryPipeline.get.result).map { case (entities, ordering) => sortEntities(entities) }
  }

  private def sortEntities(entities: Iterable[Entity]): Seq[Entity] = {
    entities.toSeq.sortWith { (e1, e2) =>
      e1.name < e2.name
    }
  }

  //Takes a list of entities at the end of a pipeline and returns them in final format.
  private def workspaceEntityFinalFunc(entityRefName: String)(context: SlickExpressionContext, shouldBeNone: Option[PipeType]): ReadAction[Iterable[Entity]] = {
    assert(shouldBeNone.isEmpty)

    val query = for {
      workspace <- workspaceQuery.findByIdQuery(context.workspaceContext.workspaceId)
      attribute <- workspaceAttributeQuery if attribute.ownerId === workspace.id && attribute.name === entityRefName
      entity <- entityQuery if attribute.valueEntityRef === entity.id
    } yield entity

    val entitiesAction = entityQuery.unmarshalEntities(entityQuery.joinOnAttributesAndRefs(query))
    entitiesAction.map { case (entities) => sortEntities(entities) }
  }

  private def stringFunc(str: String)(context: SlickExpressionContext, shouldBeNone: Option[PipeType]): ReadAction[Iterable[Attribute]] = {
    assert(shouldBeNone.isEmpty)
    DBIO.successful(Seq(AttributeString(str.drop(1).dropRight(1))))
  }

  private def floatFunc(dec: String)(context: SlickExpressionContext, shouldBeNone: Option[PipeType]): ReadAction[Iterable[Attribute]] = {
    assert(shouldBeNone.isEmpty)
    DBIO.successful(Seq(AttributeNumber(BigDecimal(dec))))
  }
}

class SlickExpressionEvaluator(parser: SlickExpressionParser)(implicit executionContext: ExecutionContext)  {
  def evalFinalAttribute(workspaceContext: SlickWorkspaceContext, rootType:String, rootName:String, expression:String): Try[ReadAction[Iterable[AttributeValue]]] = {
    parser.parseAttributeExpr(expression).map( runPipe(SlickExpressionContext(workspaceContext, rootType, rootName), _).map(_.collect {
      case AttributeNull => Seq.empty
      case AttributeEmptyList => Seq.empty
      case av: AttributeValue => Seq(av)
      case avl: AttributeValueList => avl.list
      case badType => throw new RawlsException(s"unsupported type resulting from attribute expression: $badType: ${badType.getClass}")
    }.flatten))
  }

  def evalFinalEntity(workspaceContext: SlickWorkspaceContext, rootType:String, rootName:String, expression:String): Try[ReadAction[Iterable[Entity]]] = {
    parser.parseEntityExpr(expression)
      .map( runPipe(SlickExpressionContext(workspaceContext, rootType, rootName), _).map(_.collect { case e: Entity => e } ))
  }

  private def runPipe(expressionContext: SlickExpressionContext, pipe: parser.PipelineQuery): ReadAction[Iterable[Any]] = {
    val builtPipe = pipe.rootStep.map(rootStep => pipe.steps.foldLeft(rootStep(expressionContext)){ ( queryPipeline, func ) => func(expressionContext, queryPipeline) })

    //Run the final step. This executes the pipeline and returns its output.
    Try {
      pipe.finalStep( expressionContext, builtPipe )
    } match {
      case Success(finalResult) => finalResult
      case Failure(regret) => dbio.DBIO.failed(regret)
    }
  }

}
