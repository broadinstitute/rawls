package org.broadinstitute.dsde.rawls.expressions

import java.sql.Timestamp

import org.broadinstitute.dsde.rawls.dataaccess.SlickWorkspaceContext
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model.{Attributable, Attribute, AttributeEntityReference, AttributeEntityReferenceList, AttributeName, AttributeNull, AttributeNumber, AttributeString}
import org.broadinstitute.dsde.rawls.util.CollectionUtils
import slick.dbio.Effect
import slick.profile.FixedSqlStreamingAction

import scala.util.Try
import scala.util.parsing.combinator.JavaTokenParsers

case class SlickExpressionContext(workspaceContext: SlickWorkspaceContext, rootEntities: Seq[EntityRecord], transactionId: String)

trait SlickExpressionParser extends JavaTokenParsers {
  this: DriverComponent
    with ExprEvalComponent
    with EntityComponent
    with WorkspaceComponent
    with AttributeComponent =>
  import driver.api._

  // A parsed expression will result in a PipelineQuery. Each step in a query traverses from entity to
  // entity following references. The final step takes the last entity, producing a result dependent on the query
  // (e.g. loading the entity itself, getting an attribute). The root step produces an entity to start the pipeline.
  // If rootStep is None, steps should also be empty and finalStep does all the work.
  abstract class PipelineQuery[FR <: FinalResult[_]](val rootStep: Option[RootFunc], val steps: List[PipeFunc], val finalStep: FinalFunc[FR])
  case class EntityPipelineQuery(override val rootStep: Option[RootFunc], override val steps: List[PipeFunc], override val finalStep: EntityFinalFunc) extends PipelineQuery(rootStep, steps, finalStep)
  case class AttributePipelineQuery(override val rootStep: Option[RootFunc], override val steps: List[PipeFunc], override val finalStep: AttributeFinalFunc) extends PipelineQuery(rootStep, steps, finalStep)

  // starting with just an expression context produces a PipeResult
  type RootFunc = (SlickExpressionContext) => PipeType

  // extends the input PipeType producing a PipeType that traverses further down the chain
  type PipeFunc = (SlickExpressionContext, PipeType) => PipeType

  // converts the incoming PipeType into the appropriate db action
  // PipeType may be None when there is no pipeline (e.g. workspace attributes)
  // Returns a map of entity names to an iterable of the expression result
  type FinalFunc[FR <: FinalResult[_]] = (SlickExpressionContext, Option[PipeType]) => FR
  type EntityFinalFunc = FinalFunc[FinalEntityResult]
  type AttributeFinalFunc = FinalFunc[FinalAttributeResult]

  // a query that results in the root entity's name, entity records, whether any of the entities was ever part of a list, and a sort ordering
  type PipeType = Query[(Rep[String], EntityTable, Rep[Boolean]), (String, EntityRecord, Boolean), Seq]

  // the types that FinalFuncs generate.
  // These are maps from entity name to whatever the result is.
  type FinalResult[ResultType] = ReadAction[Map[String, ResultType]]

  // For attribute expressions, attribute values w/ their owning entities and whether we exploded an entity list
  case class AttributeResult(lastEntityToAttribute: Map[String, Attribute], hasExplodedEntity: Boolean )
  type FinalAttributeResult = FinalResult[AttributeResult] //ReadAction[Map[String, AttributeResult]]

  // For entity expressions, just the appropriate entity records
  type EntityResult = Iterable[EntityRecord]
  type FinalEntityResult = FinalResult[EntityResult] //ReadAction[Map[String, Iterable[EntityRecord]]]

  /** Parser definitions **/
  // Entity expressions take the general form entity.ref.ref.attribute.
  // For now, we expect the initial entity to be the special token "this", which is bound at evaluation time to a root entity.

  //Parser for expressions ending in an attribute value (not an entity reference)
  private def attributeExpression: Parser[AttributePipelineQuery] = {
    // the basic case: this.(ref.)*attribute
    entityRootDot ~ rep(entityRefDot) ~ valueAttribute ^^ {
      case root ~ Nil ~ last => AttributePipelineQuery(Option(root), List.empty, last)
      case root ~ ref ~ last => AttributePipelineQuery(Option(root), ref, last)
    } |
    // attributes at the end of a reference chain starting at a workspace: workspace.ref.(ref.)*attribute
    workspaceEntityRefDot ~ rep(entityRefDot) ~ valueAttribute ^^ {
      case workspace ~ Nil ~ last => AttributePipelineQuery(Option(workspace), List.empty, last)
      case workspace ~ ref ~ last => AttributePipelineQuery(Option(workspace), ref, last)
    } |
    // attributes directly on a workspace: workspace.attribute
    workspaceAttribute ^^ {
      case workspace => AttributePipelineQuery(None, List.empty, workspace)
    }
  }

  //Parser for output expressions: this.attribute or workspace.attribute (no entity references in the middle)
  private def outputAttributeExpression: Parser[AttributePipelineQuery] = {
    // this.attribute
    entityRootDot ~ valueAttribute ^^ {
      case root ~ attr => AttributePipelineQuery(Option(root), List.empty, attr)
    } |
    // workspace.attribute
    workspaceAttribute ^^ {
      case workspace => AttributePipelineQuery(None, List.empty, workspace)
    }
  }

  //Parser for expressions ending in an attribute that's a reference to another entity
  private def entityExpression: Parser[EntityPipelineQuery] = {
    // reference IS the entity: this
    entityRoot ^^ {
      case root => EntityPipelineQuery(Option(root), List.empty, entityFinalFunc)
    } |
    // reference chain starting with an entity: this.(ref.)*ref
    entityRootDot ~ rep(entityRefDot) ~ entityRef ^^ {
      case root ~ Nil ~ last => EntityPipelineQuery(Option(root), List(last), entityFinalFunc)
      case root ~ ref ~ last => EntityPipelineQuery(Option(root), ref :+ last, entityFinalFunc)
    } |
    // reference chain starting with the workspace: workspace.(ref.)*ref
    workspaceEntityRefDot ~ rep(entityRefDot) ~ entityRef ^^ {
      case workspace ~ Nil ~ last => EntityPipelineQuery(Option(workspace), List(last), entityFinalFunc)
      case workspace ~ ref ~ last => EntityPipelineQuery(Option(workspace), ref :+ last, entityFinalFunc)
    } |
    // reference directly off the workspace: workspace.ref
    workspaceEntityRef ^^ {
      case workspace => EntityPipelineQuery(None, List.empty, workspace)
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
  private def workspaceAttribute: Parser[AttributeFinalFunc] =
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
  private def workspaceEntityRef: Parser[EntityFinalFunc] =
    "workspace." ~> attributeIdent ^^ { case entityRef => workspaceEntityFinalFunc(entityRef)}

  // the last attribute has no dot after it
  private def valueAttribute: Parser[AttributeFinalFunc] =
    attributeIdent ^^ {
      case attrName =>
        if (Attributable.reservedAttributeNames.contains(attrName.name)) entityReservedAttributeFinalFunc(attrName.name)
        else entityAttributeFinalFunc(attrName)
    }

  def parseAttributeExpr(expression: String): Try[AttributePipelineQuery] = {
    parse(expression, attributeExpression)
  }

  def parseOutputAttributeExpr(expression: String): Try[AttributePipelineQuery] = {
    parse(expression, outputAttributeExpression)
  }

  def parseEntityExpr(expression: String): Try[EntityPipelineQuery] = {
    parse(expression, entityExpression)
  }

  private def parse[PQ <: PipelineQuery[_]](expression: String, parser: Parser[PQ] ): Try[PQ] = {
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
    } yield (entity.name, entity, false)
  }

  // final func that gets an attribute off a workspace
  private def workspaceAttributeFinalFunc(attrName: AttributeName)(context: SlickExpressionContext, shouldBeNone: Option[PipeType]): FinalAttributeResult = {
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
      val attributesOption: Option[AttributeMap] = workspaceAttributeQuery.unmarshalAttributes(wsIdAndAttributes.map((_, None))).get(context.workspaceContext.workspaceId)
      val wsExprResult = attributesOption.map { attributes => attributes.getOrElse(attrName, AttributeNull) }.getOrElse(AttributeNull)

      //Return the value of the expression once for each entity we wanted to evaluate this expression against!
      context.rootEntities.map { rec =>
        rec.name -> AttributeResult(Map(rec.name -> wsExprResult), hasExplodedEntity = false)
      }.toMap
    }
  }

  // root func that gets an entity reference off a workspace
  private def workspaceEntityRefRootFunc(attrName: AttributeName)(context: SlickExpressionContext): PipeType = {
    for {
      rootEntity <- exprEvalQuery if rootEntity.transactionId === context.transactionId
      workspace <- workspaceQuery.findByIdQuery(context.workspaceContext.workspaceId)
      attribute <- workspaceAttributeQuery if attribute.ownerId === workspace.id && attribute.name === attrName.name && attribute.namespace === attrName.namespace
      nextEntity <- entityQuery if attribute.valueEntityRef === nextEntity.id
    } yield (rootEntity.name, nextEntity, attribute.listLength.isDefined)
  }

  // add pipe to an entity referenced by the current entity
  private def entityNameAttributePipeFunc(attrName: AttributeName)(context: SlickExpressionContext, queryPipeline: PipeType): PipeType = {
    (for {
      (rootEntityName, entity, haveExplodedEntityList) <- queryPipeline
      attribute <- entityAttributeQuery if entity.id === attribute.ownerId && attribute.name === attrName.name && attribute.namespace === attrName.namespace
      nextEntity <- entityQuery if attribute.valueEntityRef === nextEntity.id
    } yield {
      (rootEntityName, nextEntity, haveExplodedEntityList || attribute.listLength.isDefined)
  }).sortBy({case (nm, ent, seenList) => ent.name })
  }

  // filter attributes to only the given attributeName and convert to attribute
  // Return a map from the entity names to the list of attribute values for each entity
  private def entityAttributeFinalFunc(attrName: AttributeName)(context: SlickExpressionContext, queryPipeline: Option[PipeType]): FinalAttributeResult = {
    // attributeForNameQuery will only contain attribute records of the given name but for possibly more than 1 entity
    // and in the case of a list there will be more than one attribute record for an entity
    val attributeQuery = for {
      (rootEntityName, entity, haveExplodedEntityList) <- queryPipeline.get
      attribute <- entityAttributeQuery if entity.id === attribute.ownerId && attribute.name === attrName.name && attribute.namespace === attrName.namespace
    } yield (rootEntityName, entity.name, haveExplodedEntityList, attribute)

    val attributeForNameQuery =
      if (attrName.namespace.equalsIgnoreCase(AttributeName.defaultNamespace) && attrName.name.toLowerCase.endsWith(Attributable.entityIdAttributeSuffix)) {
        // This query will match an attribute with name in the form [entity type]_id and return the entity's name as an artificial
        // attribute record. The artificial record consists of all literal columns except the name in the value string spot.
        // The fighting alligators (<>) at the end allows mapping of the artificial record to the right record case class.
        val attributeIdQuery = queryPipeline.get.filter { case (rootEntityName, entity, haveExplodedEntityList) =>
          entity.entityType ++ "_id" === attrName.name && LiteralColumn(AttributeName.defaultNamespace) === attrName.namespace
        }.map { case (rootEntityName, entity, haveExplodedEntityList) =>
          (rootEntityName, entity.name, haveExplodedEntityList, (LiteralColumn(0L), LiteralColumn(0L), LiteralColumn(attrName.namespace), LiteralColumn(attrName.name), entity.name.?, Rep.None[Double], Rep.None[Boolean], Rep.None[String], Rep.None[Long], Rep.None[Int], Rep.None[Int], LiteralColumn(false), Rep.None[Timestamp]) <> (EntityAttributeRecord.tupled, EntityAttributeRecord.unapply))
        }

        // we need to do both queries because we don't know the entity type until execution time
        // and this expression could be either [entity type]_id or some_other_id
        // so do the normal query first and if that is empty do the second query, this preserves behavior of any
        // _id attributes that may have existed (I counted 4) before this change went in
        // I think using a union here would be better but https://github.com/slick/slick/issues/1571
        attributeQuery.result flatMap { (entityWithAttributeRecs: Seq[(String, String, Boolean, EntityAttributeRecord)]) =>
          if (entityWithAttributeRecs.isEmpty) attributeIdQuery.result
          else DBIO.successful(entityWithAttributeRecs)
        }

      } else {
        attributeQuery.result
      }

    attributeForNameQuery.map { entityWithAttributeRecs =>
      //converting the complex query result into this case class makes it way easier to reason about
      case class FinalAttributeQueryRecord(rootEntity: String, lastEntity: String, hasExplodedEntity: Boolean, eattrRec: EntityAttributeRecord)
      val finalQueryRecords: Seq[FinalAttributeQueryRecord] = entityWithAttributeRecs.map(FinalAttributeQueryRecord.tupled)

      val byRootEnt: Map[String, Seq[FinalAttributeQueryRecord]] = finalQueryRecords.groupBy (_.rootEntity)

      byRootEnt.map { case (rootEnt, attrs: Seq[FinalAttributeQueryRecord]) =>
        //Make a note of if we ever exploded an entity reference list while evaluating this expression.
        val haveExplodedEntityList = attrs.exists(_.hasExplodedEntity)

        // unmarshalAttributes requires a structure of ((entity id, attribute rec), option[entity rec]) where the optional entity rec is used for entity references.
        // We're evaluating an attribute expression here, so this final attribute is NOT allowed to be an entity reference.

        // Only problem is: it might be an entity reference anyway, because we can't stop the user typing "this.participant" into a method config input
        // and then running it on a sample. In this case, we have the attribute record with valueEntityRef defined and pointing to the record ID of the referenced entity,
        // but we didn't do the extra JOIN in the SQL query to find out _which_ entity it is.

        // The upshot of all this is we have to handle these dangling and unwanted entity references separately. So first we filter out the well-behaved value attributes.
        val (refAttrRecs, valueAttrRecs) = attrs.partition { faqr => isEntityRefRecord(faqr.eattrRec) }

        //Unmarshal the good ones. This is what the user actually meant.
        val attributeData = valueAttrRecs.map( faqr => ((faqr.lastEntity, faqr.eattrRec), None) )
        val attributesByEntityId: Map[String, AttributeMap] = entityAttributeQuery.unmarshalAttributes(attributeData)

        // These are the bad ones. We gather together the dangling references and make dummy EntityRef or RefList attributes for them.
        val refAttributesByEntityId: Map[String, AttributeMap] = refAttrRecs.groupBy(_.lastEntity) map { case (attrEnt, groupSeq: Seq[FinalAttributeQueryRecord]) =>
          val attrRec = groupSeq.head.eattrRec
          if( groupSeq.size == 1 ) {
            attrEnt -> Map(AttributeName(attrRec.namespace, attrRec.name) -> AttributeEntityReference(entityType = "BAD REFERENCE", entityName = "BAD REFERENCE"))
          } else {
            attrEnt -> Map(AttributeName(attrRec.namespace, attrRec.name) -> AttributeEntityReferenceList( Seq.fill(groupSeq.size)(AttributeEntityReference(entityType = "BAD REFERENCE", entityName = "BAD REFERENCE"))) )
          }
        }

        // The only piece of good news in all this nonsense is we know the IDs won't clash when we ++ the two maps together,
        // because we enforce consistent value/ref typing when we append list members.
        val namedAttributesOnlyByEntityId = (attributesByEntityId ++ refAttributesByEntityId).map({ case (k, v) => k -> v.getOrElse(attrName, AttributeNull) }).toSeq

        rootEnt -> AttributeResult(namedAttributesOnlyByEntityId.toMap, haveExplodedEntityList)
      }
    }
  }

  // final func that handles reserved attributes on an entity
  private def entityReservedAttributeFinalFunc(attributeName: String)(context: SlickExpressionContext, queryPipeline: Option[PipeType]): FinalAttributeResult = {
    //Given a single entity record, extract either name or entityType from it.
    def extractNameFromRecord( rec: EntityRecord ) = { AttributeString(rec.name) }
    def extractEntityTypeFromRecord( rec: EntityRecord ) = { AttributeString(rec.entityType) }

    //Helper function to extract the entity at the end of the pipeline and yank its name or entityType, then convert into the result type
    def finalAttributeResult( baseQuery: PipeType, recordExtractionFn: EntityRecord => Attribute ): FinalAttributeResult = {
      baseQuery.sortBy(_._2.name).distinct.result map { queryRes: Seq[(String, EntityRecord, Boolean)] =>
        CollectionUtils.groupByTriples(queryRes).map{ case (rootEntityName, entityRecords: Seq[(EntityRecord, Boolean)]) =>
          val haveExplodedEntityList = entityRecords.exists(_._2)
          rootEntityName -> AttributeResult((entityRecords map { case (rec: EntityRecord, _) =>
            rec.name -> recordExtractionFn(rec)
          }).toMap, haveExplodedEntityList)
        }
      }
    }

    //Might as well call the function now it exists.
    attributeName match {
      case Attributable.nameReservedAttribute => finalAttributeResult(queryPipeline.get, extractNameFromRecord)
      case Attributable.entityTypeReservedAttribute => finalAttributeResult(queryPipeline.get, extractEntityTypeFromRecord)
    }
  }

  // final func that handles reserved attributes on a workspace
  private def workspaceReservedAttributeFinalFunc(attributeName: String)(context: SlickExpressionContext, shouldBeNone: Option[PipeType]): FinalAttributeResult = {
    assert(shouldBeNone.isEmpty)

    attributeName match {
      case Attributable.nameReservedAttribute | Attributable.workspaceIdAttribute =>
        DBIO.successful(
          context.rootEntities.map { rootEnt =>
            rootEnt.name -> AttributeResult(Map(rootEnt.name -> AttributeString(context.workspaceContext.workspace.name)), hasExplodedEntity = false)
          }.toMap )
      case Attributable.entityTypeReservedAttribute =>
        DBIO.successful(
          context.rootEntities.map { rootEnt =>
            rootEnt.name -> AttributeResult(Map(rootEnt.name -> AttributeString(Attributable.workspaceEntityType)), hasExplodedEntity = false)
          }.toMap )
    }
  }

  //Takes a list of entities at the end of a pipeline and returns them in final format.
  private def entityFinalFunc(context: SlickExpressionContext, queryPipeline: Option[PipeType]): FinalEntityResult = {
    val pipeline: ReadAction[Seq[(String, EntityRecord, Boolean)]] = queryPipeline.get.sortBy(_._2.name.asc).result
    pipeline.map { elem: Seq[(String, EntityRecord, Boolean)] =>
      CollectionUtils.groupByTuples(elem.map(e => (e._1, e._2)))
    }
  }

  //Takes a list of entities at the end of a pipeline and returns them in final format.
  private def workspaceEntityFinalFunc(attrName: AttributeName)(context: SlickExpressionContext, shouldBeNone: Option[PipeType]): FinalEntityResult = {
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
}
