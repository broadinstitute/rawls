package org.broadinstitute.dsde.rawls.expressions

import java.util.UUID

import _root_.slick.dbio
import org.broadinstitute.dsde.rawls.util.CollectionUtils
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.model._

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}
import scala.util.parsing.combinator.JavaTokenParsers

case class SlickExpressionContext(workspaceContext: SlickWorkspaceContext, rootEntities: Seq[EntityRecord], transactionId: String)

trait SlickExpressionParser extends JavaTokenParsers {
  this: DriverComponent
    with EntityComponent
    with WorkspaceComponent
    with AttributeComponent =>
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
  // Returns a map of entity names to an iterable of the expression result
  type FinalFunc = (SlickExpressionContext, Option[PipeType]) => ReadAction[Map[String, Iterable[Any]]]

  // a query that results in the root entity's name, entity records and a sort ordering
  type PipeType = Query[(Rep[String], EntityTable), (String, EntityRecord), Seq]

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

  // matches some_attr_namespace:attr_name or attr_name
  private val attributeIdent: Parser[AttributeName] = opt(ident ~ ":") ~ ident ^^ {
    case Some(namespace ~ ":") ~ name => AttributeName(namespace, name)
    case None ~ name => AttributeName.withDefaultNS(name)
  }

  // workspace.attribute, note that this is a FinalFunc - because workspaces are not entities they can be piped the same way
  private def workspaceAttribute: Parser[FinalFunc] =
    "workspace." ~> attributeIdent ^^ {
      case attrName =>
        if (Attributable.reservedAttributeNames.contains(attrName.name)) workspaceReservedAttributeFinalFunc(attrName.name)
        else workspaceAttributeFinalFunc(attrName)
    }

  // an entity reference as the final attribute in an expression
  private def entityRef: Parser[PipeFunc] =
    attributeIdent ^^ { case entityRef => entityNameAttributePipeFunc(entityRef)}

  // an entity reference in the middle of an expression
  private def entityRefDot: Parser[PipeFunc] =
    attributeIdent <~ "." ^^ { case entityRef => entityNameAttributePipeFunc(entityRef)}

  // an entity reference after the workspace, this can be a RootFunc because it resolves to an entity query that can be piped
  private def workspaceEntityRefDot: Parser[RootFunc] =
    "workspace." ~> attributeIdent <~ "." ^^ { case entityRef => workspaceEntityRefRootFunc(entityRef)}

  // an entity reference after the workspace, note that this is a FinalFunc - because workspaces are not entities they can be piped the same way
  private def workspaceEntityRef: Parser[FinalFunc] =
    "workspace." ~> attributeIdent ^^ { case entityRef => workspaceEntityFinalFunc(entityRef)}

  // the last attribute has no dot after it
  private def valueAttribute: Parser[FinalFunc] =
    attributeIdent ^^ {
      case attrName =>
        if (Attributable.reservedAttributeNames.contains(attrName.name)) entityReservedAttributeFinalFunc(attrName.name)
        else entityAttributeFinalFunc(attrName)
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
      rootEntities <- exprEvalQuery if rootEntities.transactionId === context.transactionId
      entity <- entityQuery if rootEntities.id === entity.id
    } yield (entity.name, entity)
  }

  // final func that gets an attribute off a workspace
  private def workspaceAttributeFinalFunc(attrName: AttributeName)(context: SlickExpressionContext, shouldBeNone: Option[PipeType]): ReadAction[Map[String,Iterable[Attribute]]] = {
    assert(shouldBeNone.isEmpty)

    val wsIdAndAttributeQuery = for {
      workspace <- workspaceQuery.findByIdQuery(context.workspaceContext.workspaceId)
      attribute <- workspaceAttributeQuery if attribute.ownerId === workspace.id && attribute.name === attrName.name && attribute.namespace === attrName.namespace
    } yield (workspace.id, attribute)

    wsIdAndAttributeQuery.result.map { wsIdAndAttributes =>
      // the query restricts all results to have workspace id === context.workspaceContext.workspaceId
      // unmarshalAttributes requires a structure of ((ws id, attribute rec), option[entity rec]) where
      // the optional entity rec is used for references. Since we know we are not dealing with a reference here
      // as this is the attribute final func, we can pass in None.
      val attributesOption = workspaceAttributeQuery.unmarshalAttributes(wsIdAndAttributes.map((_, None))).get(context.workspaceContext.workspaceId)
      val wsExprResult = attributesOption.map { attributes => Seq(attributes.getOrElse(attrName, AttributeNull)) }.getOrElse(Seq.empty)

      //Return the value of the expression once for each entity we wanted to evaluate this expression against!
      context.rootEntities.map( rec => (rec.name, wsExprResult) ).toMap
    }
  }

  // root func that gets an entity reference off a workspace
  private def workspaceEntityRefRootFunc(attrName: AttributeName)(context: SlickExpressionContext): PipeType = {
    for {
      rootEntity <- exprEvalQuery if rootEntity.transactionId === context.transactionId
      workspace <- workspaceQuery.findByIdQuery(context.workspaceContext.workspaceId)
      attribute <- workspaceAttributeQuery if attribute.ownerId === workspace.id && attribute.name === attrName.name && attribute.namespace === attrName.namespace
      nextEntity <- entityQuery if attribute.valueEntityRef === nextEntity.id
    } yield (rootEntity.name, nextEntity)
  }

  // add pipe to an entity referenced by the current entity
  private def entityNameAttributePipeFunc(attrName: AttributeName)(context: SlickExpressionContext, queryPipeline: PipeType): PipeType = {
    (for {
      (rootEntityName, entity) <- queryPipeline
      attribute <- entityAttributeQuery if entity.id === attribute.ownerId && attribute.name === attrName.name && attribute.namespace === attrName.namespace
      nextEntity <- entityQuery if attribute.valueEntityRef === nextEntity.id
    } yield (rootEntityName, nextEntity)).sortBy({case (nm, ent) => ent.name })
  }

  // filter attributes to only the given attributeName and convert to attribute
  // Return a map from the entity names to the list of attribute values for each entity
  private def entityAttributeFinalFunc(attrName: AttributeName)(context: SlickExpressionContext, queryPipeline: Option[PipeType]): ReadAction[Map[String, Iterable[Attribute]]] = {
    // attributeForNameQuery will only contain attribute records of the given name but for possibly more than 1 entity
    // and in the case of a list there will be more than one attribute record for an entity
    val attributeForNameQuery = (for {
      (rootEntityName, entity) <- queryPipeline.get
      attribute <- entityAttributeQuery if entity.id === attribute.ownerId && attribute.name === attrName.name && attribute.namespace === attrName.namespace
    } yield (rootEntityName, entity.name, attribute)).result

    attributeForNameQuery.map { entityWithAttributeRecs =>
      val byRootEnt: Map[String, Seq[(String, String, EntityAttributeRecord)]] = entityWithAttributeRecs groupBy { case (rootEntity, lastEntity, attribRecord) => rootEntity }

      byRootEnt.map { case (rootEnt, attrs) =>
        // unmarshalAttributes requires a structure of ((entity id, attribute rec), option[entity rec]) where
        // the optional entity rec is used for references. Since we know we are not dealing with a reference here
        // as this is the attribute final func, we can pass in None.
        val attributesByEntityId = entityAttributeQuery.unmarshalAttributes(attrs.map { case (root, attrEnt, attrRec) => ((attrEnt, attrRec), None) })

        val namedAttributesOnlyByEntityId = attributesByEntityId.map({ case (k, v) => k -> v.getOrElse(attrName, AttributeNull) }).toSeq
        // need to sort here because some of the manipulations above don't preserve order so we can't sort in the query
        val orderedEntityNameAndAttributes = namedAttributesOnlyByEntityId.sortWith { case ((entityName1, _), (entityName2, _)) =>
          entityName1 < entityName2
        }

        rootEnt -> orderedEntityNameAndAttributes.map { case (_, attribute) => attribute }
      }
    }
  }

  // final func that handles reserved attributes on an entity
  private def entityReservedAttributeFinalFunc(attributeName: String)(context: SlickExpressionContext, queryPipeline: Option[PipeType]): ReadAction[Map[String,Iterable[Attribute]]] = {
    //Given a single entity record, extract either name or entityType from it.
    def extractNameFromRecord( rec: EntityRecord ) = { AttributeString(rec.name) }
    def extractEntityTypeFromRecord( rec: EntityRecord ) = { AttributeString(rec.entityType) }

    //Helper function to group the result nicely and extract either name or entityType from the record as you please.
    def returnMapOfRootEntityToReservedAttribute( baseQuery: PipeType, recordExtractionFn: EntityRecord => Attribute ): ReadAction[Map[String,Iterable[Attribute]]] = {
      baseQuery.sortBy(_._2.name).distinct.result map { queryRes: Seq[(String, EntityRecord)] =>
        CollectionUtils.groupByTuples(queryRes).map({ case (k, v) => k -> v.map(recordExtractionFn(_)) })
      }
    }

    //Might as well call the function now it exists.
    attributeName match {
      case "name" => returnMapOfRootEntityToReservedAttribute(queryPipeline.get, extractNameFromRecord)
      case "entityType" => returnMapOfRootEntityToReservedAttribute(queryPipeline.get, extractEntityTypeFromRecord)
    }
  }

  // final func that handles reserved attributes on a workspace
  private def workspaceReservedAttributeFinalFunc(attributeName: String)(context: SlickExpressionContext, shouldBeNone: Option[PipeType]): ReadAction[Map[String,Iterable[Attribute]]] = {
    assert(shouldBeNone.isEmpty)

    attributeName match {
      case "name" => DBIO.successful( context.rootEntities.map( _.name -> Seq(AttributeString(context.workspaceContext.workspace.name))).toMap )
      case "entityType" => throw new RawlsException("entityType not valid for workspace")
    }
  }

  //Takes a list of entities at the end of a pipeline and returns them in final format.
  private def entityFinalFunc(context: SlickExpressionContext, queryPipeline: Option[PipeType]): ReadAction[Map[String,Iterable[EntityRecord]]] = {
    queryPipeline.get.sortBy(_._2.name.asc).result.map(CollectionUtils.groupByTuples)
  }

  //Takes a list of entities at the end of a pipeline and returns them in final format.
  private def workspaceEntityFinalFunc(attrName: AttributeName)(context: SlickExpressionContext, shouldBeNone: Option[PipeType]): ReadAction[Map[String,Iterable[EntityRecord]]] = {
    assert(shouldBeNone.isEmpty)

    val query = for {
      workspace <- workspaceQuery.findByIdQuery(context.workspaceContext.workspaceId)
      attribute <- workspaceAttributeQuery if attribute.ownerId === workspace.id && attribute.name === attrName.name && attribute.namespace === attrName.namespace
      entity <- entityQuery if attribute.valueEntityRef === entity.id
    } yield entity

    //Return the value of the expression once for each entity we wanted to evaluate this expression against!
    query.sortBy(_.name.asc).result map { resultEntities =>
      (for {
        entity <- context.rootEntities
      } yield (entity.name, resultEntities)).toMap
    }
  }

  private def stringFunc(str: String)(context: SlickExpressionContext, shouldBeNone: Option[PipeType]): ReadAction[Map[String,Iterable[Attribute]]] = {
    assert(shouldBeNone.isEmpty)
    DBIO.successful( context.rootEntities.map( _.name -> Seq(AttributeString(str.drop(1).dropRight(1)))).toMap )
  }

  private def floatFunc(dec: String)(context: SlickExpressionContext, shouldBeNone: Option[PipeType]): ReadAction[Map[String,Iterable[Attribute]]] = {
    assert(shouldBeNone.isEmpty)
    DBIO.successful( context.rootEntities.map( _.name -> Seq(AttributeNumber(BigDecimal(dec)))).toMap )
  }
}

object SlickExpressionEvaluator {
  def withNewExpressionEvaluator[R](parser: DataAccess, rootEntities: Seq[EntityRecord])
                                   (op: SlickExpressionEvaluator => ReadWriteAction[R])
                                   (implicit executionContext: ExecutionContext): ReadWriteAction[R] = {
    val evaluator = new SlickExpressionEvaluator(parser, rootEntities)

    evaluator.populateExprEvalScratchTable() andThen
      op(evaluator) andFinally
      evaluator.clearExprEvalScratchTable()
  }

  def withNewExpressionEvaluator[R](parser: DataAccess, workspaceContext: SlickWorkspaceContext, rootType: String, rootName: String)
                                   (op: SlickExpressionEvaluator => ReadWriteAction[R])
                                   (implicit executionContext: ExecutionContext): ReadWriteAction[R] = {
    import parser.driver.api._

    //Find the root entity for the expression
    val dbRootEntityRec = parser.entityQuery.findEntityByName(workspaceContext.workspaceId, rootType, rootName).result

    //Sanity check that we've only got one, and then pass upwards
    dbRootEntityRec flatMap { rootEntityRec =>
      if(rootEntityRec.size != 1) {
        DBIO.failed(new RawlsException(s"Found != 1 root entity when searching for ${rootType}/$rootName"))
      } else {
        withNewExpressionEvaluator(parser, rootEntityRec)(op)
      }
    }
  }
}

class SlickExpressionEvaluator protected (val parser: DataAccess, val rootEntities: Seq[EntityRecord])(implicit executionContext: ExecutionContext) {
  import parser.driver.api._

  val transactionId = UUID.randomUUID().toString

  def populateExprEvalScratchTable() = {
    val exprEvalBatches = rootEntities.map( e => parser.ExprEvalRecord(e.id, e.name, transactionId) ).grouped(parser.batchSize)

    DBIO.sequence(exprEvalBatches.toSeq.map(batch => parser.exprEvalQuery ++= batch))
  }

  def clearExprEvalScratchTable() = {
    parser.exprEvalQuery.filter(_.transactionId === transactionId).delete
  }

  def evalFinalAttribute(workspaceContext: SlickWorkspaceContext, expression: String): ReadWriteAction[Map[String, Try[Iterable[AttributeValue]]]] = {
    parser.parseAttributeExpr(expression) match {
      case Failure(regret) => DBIO.failed(new RawlsException(regret.getMessage))
      case Success(expr) =>
        runPipe(SlickExpressionContext(workspaceContext, rootEntities, transactionId), expr) map { exprResults =>
          val results = exprResults map { case (key, attrVals) =>
            key -> Try(attrVals.collect {
              case AttributeNull => Seq.empty
              case AttributeEmptyList => Seq.empty
              case av: AttributeValue => Seq(av)
              case avl: AttributeValueList => avl.list
              case badType => throw new RawlsException(s"unsupported type resulting from attribute expression: $badType: ${badType.getClass}")
            }.flatten)
          }
          //add any missing entities (i.e. those missing the attribute) back into the result map
          results ++ rootEntities.map(_.name).filterNot( results.keySet.contains ).map { missingKey => missingKey -> Success(Seq()) }
        }
    }
  }

  //This is boiling away the Try associated with attempting to parse the expression. Is this OK?
  def evalFinalEntity(workspaceContext: SlickWorkspaceContext, expression:String): ReadWriteAction[Iterable[EntityRecord]] = {
      if( rootEntities.isEmpty ) {
        DBIO.failed(new RawlsException(s"ExpressionEvaluator has no entities passed to evalFinalEntity $expression"))
      } else if( rootEntities.size > 1 ) {
        DBIO.failed(new RawlsException(s"ExpressionEvaluator has been set up with ${rootEntities.size} entities for evalFinalEntity, can only accept 1."))
      } else {
        //Try to parse the expression
        val parsedExpr: Try[parser.PipelineQuery] = parser.parseEntityExpr(expression)

        parsedExpr match {
          //fail out if we couldn't parse the expression
          case Failure(regret) => DBIO.failed(regret)
          case Success(expr) =>
            //If parsing succeeded, evaluate the expression using the given root entities and retype back to EntityRecord
            runPipe(SlickExpressionContext(workspaceContext, rootEntities, transactionId), expr).map({ resultMap =>
              //NOTE: As per the DBIO.failed a few lines up, resultMap should only have one key, the same root elem.
              val (rootElem, elems) = resultMap.head
              elems.collect { case e: EntityRecord => e }
            })
        }
    }
  }

  private def runPipe(expressionContext: SlickExpressionContext, pipe: parser.PipelineQuery): ReadAction[Map[String, Iterable[Any]]] = {
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
