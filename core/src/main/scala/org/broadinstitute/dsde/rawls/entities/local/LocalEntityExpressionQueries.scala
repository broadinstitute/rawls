package org.broadinstitute.dsde.rawls.entities.local

import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model.{
  Attributable,
  Attribute,
  AttributeEntityReference,
  AttributeEntityReferenceList,
  AttributeName,
  AttributeNull,
  AttributeString,
  Workspace
}
import org.broadinstitute.dsde.rawls.util.CollectionUtils

import java.sql.Timestamp

case class LocalEntityExpressionContext(workspaceContext: Workspace,
                                        rootEntities: Option[Seq[EntityRecord]],
                                        transactionId: String
) {
  def rootEntityNames(): Seq[String] = rootEntities match {
    case Some(entities) => entities.map(_.name)
    case None           => Seq("")
  }
}

trait LocalEntityExpressionQueries {
  this: DriverComponent with ExprEvalComponent with WorkspaceComponent with EntityComponent with AttributeComponent =>

  import driver.api._

  type PipelineStepQuery = Query[(Rep[String], EntityTable), (String, EntityRecord), Seq]

  object entityExpressionQuery {
    // the root function starts the pipeline at some root entity type in the workspace
    def entityRootQuery(context: LocalEntityExpressionContext): PipelineStepQuery =
      for {
        rootEntities <- exprEvalQuery if rootEntities.transactionId === context.transactionId
        entity <- entityQuery if rootEntities.id === entity.id
      } yield (entity.name, entity)

    // final func that gets an attribute off a workspace
    def workspaceAttributeFinalQuery(attrName: AttributeName)(context: LocalEntityExpressionContext,
                                                              shouldBeNone: Option[PipelineStepQuery]
    ): ReadAction[Map[String, Iterable[Attribute]]] = {
      assert(shouldBeNone.isEmpty)

      val wsIdAndAttributeQuery = for {
        workspace <- workspaceQuery.findByIdQuery(context.workspaceContext.workspaceIdAsUUID)
        attribute <- workspaceAttributeQuery
        if attribute.ownerId === workspace.id && attribute.name === attrName.name && attribute.namespace === attrName.namespace
      } yield (workspace.id, attribute)

      wsIdAndAttributeQuery.result.map { wsIdAndAttributes =>
        // the query restricts all results to have workspace id === context.workspaceContext.workspaceId
        // unmarshalAttributes requires a structure of ((ws id, attribute rec), option[entity rec]) where
        // the optional entity rec is used for references. Since we know we are not dealing with a reference here
        // as this is the attribute final func, we can pass in None.
        val attributesOption = workspaceAttributeQuery
          .unmarshalAttributes(wsIdAndAttributes.map((_, None)))
          .get(context.workspaceContext.workspaceIdAsUUID)
        val wsExprResult =
          attributesOption.map(attributes => Seq(attributes.getOrElse(attrName, AttributeNull))).getOrElse(Seq.empty)

        // Return the value of the expression once for each entity we wanted to evaluate this expression against!
        context.rootEntityNames().map(name => (name, wsExprResult)).toMap
      }
    }

    // root func that gets an entity reference off a workspace
    def workspaceEntityRefRootQuery(attrName: AttributeName)(context: LocalEntityExpressionContext): PipelineStepQuery =
      for {
        rootEntity <- exprEvalQuery if rootEntity.transactionId === context.transactionId
        workspace <- workspaceQuery.findByIdQuery(context.workspaceContext.workspaceIdAsUUID)
        attribute <- workspaceAttributeQuery
        if attribute.ownerId === workspace.id && attribute.name === attrName.name && attribute.namespace === attrName.namespace
        nextEntity <- entityQuery if attribute.valueEntityRef === nextEntity.id
      } yield (rootEntity.name, nextEntity)

    // add pipe to an entity referenced by the current entity
    def entityNameAttributeRelationQuery(
      attrName: AttributeName
    )(context: LocalEntityExpressionContext, queryPipeline: PipelineStepQuery): PipelineStepQuery =
      (for {
        (rootEntityName, entity) <- queryPipeline
        attribute <- entityAttributeShardQuery(context.workspaceContext)
        if entity.id === attribute.ownerId && attribute.name === attrName.name && attribute.namespace === attrName.namespace
        nextEntity <- entityQuery if attribute.valueEntityRef === nextEntity.id
      } yield (rootEntityName, nextEntity)).sortBy { case (nm, ent) => ent.name }

    // filter attributes to only the given attributeName and convert to attribute
    // Return a map from the entity names to the list of attribute values for each entity
    def entityAttributeFinalQuery(attrName: AttributeName)(context: LocalEntityExpressionContext,
                                                           queryPipeline: Option[PipelineStepQuery]
    ): ReadAction[Map[String, Iterable[Attribute]]] = {
      // attributeForNameQuery will only contain attribute records of the given name but for possibly more than 1 entity
      // and in the case of a list there will be more than one attribute record for an entity
      val attributeQuery = for {
        (rootEntityName, entity) <- queryPipeline.get
        attribute <- entityAttributeShardQuery(context.workspaceContext)
        if entity.id === attribute.ownerId && attribute.name === attrName.name && attribute.namespace === attrName.namespace
      } yield (rootEntityName, entity.name, attribute)

      val attributeForNameQuery =
        if (
          attrName.namespace.equalsIgnoreCase(AttributeName.defaultNamespace) && attrName.name.toLowerCase.endsWith(
            Attributable.entityIdAttributeSuffix
          )
        ) {
          // This query will match an attribute with name in the form [entity type]_id and return the entity's name as an artificial
          // attribute record. The artificial record consists of all literal columns except the name in the value string spot.
          // The fighting alligators (<>) at the end allows mapping of the artificial record to the right record case class.
          val attributeIdQuery = queryPipeline.get
            .filter { case (rootEntityName, entity) =>
              entity.entityType ++ "_id" === attrName.name && LiteralColumn(
                AttributeName.defaultNamespace
              ) === attrName.namespace
            }
            .map { case (rootEntityName, entity) =>
              (rootEntityName,
               entity.name,
               (LiteralColumn(0L),
                LiteralColumn(0L),
                LiteralColumn(attrName.namespace),
                LiteralColumn(attrName.name),
                entity.name.?,
                Rep.None[Double],
                Rep.None[Boolean],
                Rep.None[String],
                Rep.None[Long],
                Rep.None[Int],
                Rep.None[Int],
                LiteralColumn(false),
                Rep.None[Timestamp]
               ) <> (EntityAttributeRecord.tupled, EntityAttributeRecord.unapply)
              )
            }

          // we need to do both queries because we don't know the entity type until execution time
          // and this expression could be either [entity type]_id or some_other_id
          // so do the normal query first and if that is empty do the second query, this preserves behavior of any
          // _id attributes that may have existed (I counted 4) before this change went in
          // I think using a union here would be better but https://github.com/slick/slick/issues/1571
          attributeQuery.result flatMap { entityWithAttributeRecs =>
            if (entityWithAttributeRecs.isEmpty) attributeIdQuery.result
            else DBIO.successful(entityWithAttributeRecs)
          }

        } else {
          attributeQuery.result
        }

      attributeForNameQuery.map { entityWithAttributeRecs =>
        val byRootEnt: Map[String, Seq[(String, String, EntityAttributeRecord)]] = entityWithAttributeRecs.groupBy {
          case (rootEntity, lastEntity, attribRecord) => rootEntity
        }

        byRootEnt.map { case (rootEnt, attrs) =>
          // unmarshalAttributes requires a structure of ((entity id, attribute rec), option[entity rec]) where the optional entity rec is used for entity references.
          // We're evaluating an attribute expression here, so this final attribute is NOT allowed to be an entity reference.

          // Only problem is: it might be an entity reference anyway, because we can't stop the user typing "this.participant" into a method config input
          // and then running it on a sample. In this case, we have the attribute record with valueEntityRef defined and pointing to the record ID of the referenced entity,
          // but we didn't do the extra JOIN in the SQL query to find out _which_ entity it is.

          // The upshot of all this is we have to handle these dangling and unwanted entity references separately. So first we filter out the well-behaved value attributes.
          val (refAttrRecs, valueAttrRecs) = attrs.partition { case (root, attrEnt, attrRec) =>
            isEntityRefRecord(attrRec)
          }

          // Unmarshal the good ones. This is what the user actually meant.
          val attributesByEntityId: Map[String, AttributeMap] =
            entityAttributeShardQuery(context.workspaceContext).unmarshalAttributes(valueAttrRecs.map {
              case (root, attrEnt, attrRec) => ((attrEnt, attrRec), None)
            })

          // These are the bad ones. We gather together the dangling references and make dummy EntityRef or RefList attributes for them.
          val refAttributesByEntityId: Map[String, AttributeMap] = refAttrRecs.groupBy {
            case (root, attrEnt, attrRec) => attrEnt
          } map { case (attrEnt, groupSeq: Seq[(String, String, EntityAttributeRecord)]) =>
            val (_, _, attrRec) = groupSeq.head
            if (groupSeq.size == 1) {
              attrEnt -> Map(
                AttributeName(attrRec.namespace, attrRec.name) -> AttributeEntityReference(entityType = "BAD REFERENCE",
                                                                                           entityName = "BAD REFERENCE"
                )
              )
            } else {
              attrEnt -> Map(
                AttributeName(attrRec.namespace, attrRec.name) -> AttributeEntityReferenceList(
                  Seq.fill(groupSeq.size)(
                    AttributeEntityReference(entityType = "BAD REFERENCE", entityName = "BAD REFERENCE")
                  )
                )
              )
            }
          }

          // The only piece of good news in all this nonsense is we know the IDs won't clash when we ++ the two maps together,
          // because we enforce consistent value/ref typing when we append list members.
          val namedAttributesOnlyByEntityId = (attributesByEntityId ++ refAttributesByEntityId).map { case (k, v) =>
            k -> v.getOrElse(attrName, AttributeNull)
          }.toSeq
          // need to sort here because some of the manipulations above don't preserve order so we can't sort in the query
          val orderedEntityNameAndAttributes = namedAttributesOnlyByEntityId.sortWith {
            case ((entityName1, _), (entityName2, _)) =>
              entityName1 < entityName2
          }

          rootEnt -> orderedEntityNameAndAttributes.map { case (_, attribute) => attribute }
        }
      }
    }

    // final func that handles reserved attributes on an entity
    def entityReservedAttributeFinalQuery(attributeName: String)(context: LocalEntityExpressionContext,
                                                                 queryPipeline: Option[PipelineStepQuery]
    ): ReadAction[Map[String, Iterable[Attribute]]] = {
      // Given a single entity record, extract either name or entityType from it.
      def extractNameFromRecord(rec: EntityRecord) =
        AttributeString(rec.name)

      def extractEntityTypeFromRecord(rec: EntityRecord) =
        AttributeString(rec.entityType)

      // Helper function to group the result nicely and extract either name or entityType from the record as you please.
      def returnMapOfRootEntityToReservedAttribute(baseQuery: PipelineStepQuery,
                                                   recordExtractionFn: EntityRecord => Attribute
      ): ReadAction[Map[String, Iterable[Attribute]]] =
        baseQuery.sortBy(_._2.name).distinct.result map { queryRes: Seq[(String, EntityRecord)] =>
          CollectionUtils.groupByTuples(queryRes).map { case (k, v) => k -> v.map(recordExtractionFn(_)) }
        }

      // Might as well call the function now it exists.
      attributeName match {
        case Attributable.nameReservedAttribute =>
          returnMapOfRootEntityToReservedAttribute(queryPipeline.get, extractNameFromRecord)
        case Attributable.entityTypeReservedAttribute =>
          returnMapOfRootEntityToReservedAttribute(queryPipeline.get, extractEntityTypeFromRecord)
      }
    }

    // final func that handles reserved attributes on a workspace
    def workspaceReservedAttributeFinalQuery(attributeName: String)(context: LocalEntityExpressionContext,
                                                                    shouldBeNone: Option[PipelineStepQuery]
    ): ReadAction[Map[String, Iterable[Attribute]]] = {
      assert(shouldBeNone.isEmpty)

      attributeName match {
        case Attributable.nameReservedAttribute | Attributable.workspaceIdAttribute =>
          DBIO.successful(context.rootEntityNames().map(_ -> Seq(AttributeString(context.workspaceContext.name))).toMap)
        case Attributable.entityTypeReservedAttribute =>
          DBIO.successful(
            context.rootEntityNames().map(_ -> Seq(AttributeString(Attributable.workspaceEntityType))).toMap
          )
      }
    }

    // Takes a list of entities at the end of a pipeline and returns them in final format.
    def entityFinalQuery(context: LocalEntityExpressionContext,
                         queryPipeline: Option[PipelineStepQuery]
    ): ReadAction[Map[String, Iterable[EntityRecord]]] =
      queryPipeline.get.sortBy(_._2.name.asc).result.map(CollectionUtils.groupByTuples)

    // Takes a list of entities at the end of a pipeline and returns them in final format.
    def workspaceEntityFinalQuery(attrName: AttributeName)(context: LocalEntityExpressionContext,
                                                           shouldBeNone: Option[PipelineStepQuery]
    ): ReadAction[Map[String, Iterable[EntityRecord]]] = {
      assert(shouldBeNone.isEmpty)

      val query = for {
        workspace <- workspaceQuery.findByIdQuery(context.workspaceContext.workspaceIdAsUUID)
        attribute <- workspaceAttributeQuery
        if attribute.ownerId === workspace.id && attribute.name === attrName.name && attribute.namespace === attrName.namespace
        entity <- entityQuery if attribute.valueEntityRef === entity.id
      } yield entity

      // Return the value of the expression once for each entity we wanted to evaluate this expression against!
      query.sortBy(_.name.asc).result map { resultEntities =>
        (for {
          entityName <- context.rootEntityNames()
        } yield (entityName, resultEntities)).toMap
      }
    }
  }
}
