package org.broadinstitute.dsde.rawls.entities.opensearch

import com.typesafe.scalalogging.LazyLogging
import io.opencensus.trace.Span
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.entities.base.ExpressionEvaluationSupport.LookupExpression
import org.broadinstitute.dsde.rawls.entities.base._
import org.broadinstitute.dsde.rawls.entities.exceptions.UnsupportedEntityOperationException
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.GatherInputsResult
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.EntityUpdateDefinition
import org.broadinstitute.dsde.rawls.model.{Attribute, AttributeEntityReference, AttributeFormat, AttributeName, AttributeString, AttributeValue, Entity, EntityQuery, EntityQueryResponse, EntityTypeMetadata, JsonSupport, PlainArrayAttributeListSerializer, SubmissionValidationEntityInputs, Workspace, WorkspaceJsonSupport}
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.index.IndexRequest
import org.opensearch.client.core.CountRequest
import org.opensearch.client.{RequestOptions, RestHighLevelClient}
import org.opensearch.client.indices.{CreateIndexRequest, GetIndexRequest}
import spray.json._
import spray.json.DefaultJsonProtocol._
import org.opensearch.common.xcontent.XContentType

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class OpenSearchEntityProvider(requestArguments: EntityRequestArguments, client: RestHighLevelClient)
                              (implicit protected val executionContext: ExecutionContext)
  extends WorkspaceJsonSupport with EntityProvider with LazyLogging with ExpressionEvaluationSupport {

  override implicit val attributeFormat: AttributeFormat = new AttributeFormat with PlainArrayAttributeListSerializer

  override val entityStoreId: Option[String] = Option("OpenSearch")

  override def entityTypeMetadata(useCache: Boolean = false): Future[Map[String, EntityTypeMetadata]] = {
    val req = new ClusterHealthRequest()
    val resp = client.cluster().health(req, RequestOptions.DEFAULT)
    Future(Map("clusterhealth" -> EntityTypeMetadata(0, resp.toString, Seq())))
  }

  override def createEntity(entity: Entity): Future[Entity] =
    throw new UnsupportedEntityOperationException("create entity not supported by this provider.")

  override def deleteEntities(entityRefs: Seq[AttributeEntityReference]): Future[Int] =
    throw new UnsupportedEntityOperationException("delete entities not supported by this provider.")


  override def getEntity(entityType: String, entityName: String): Future[Entity] = {
    val indexname = requestArguments.workspace.workspaceId

    //Getting back the document
    val getRequest = new GetRequest(indexname, s"${entityType}/${entityName}")
    val response = client.get(getRequest, RequestOptions.DEFAULT)



    logger.info(s"OpenSearch get response: $response found")

    val fakeAttrs: Map[AttributeName, Attribute] = Map(AttributeName.withDefaultNS("opensearch-exists")-> AttributeString(s"${response.isExists}"))

    Future(Entity(name = entityName, entityType = entityType, attributes = fakeAttrs))

  }

  override def queryEntities(entityType: String, incomingQuery: EntityQuery, parentSpan: Span = null): Future[EntityQueryResponse] = ???

  override def evaluateExpressions(expressionEvaluationContext: ExpressionEvaluationContext, gatherInputsResult: GatherInputsResult, workspaceExpressionResults: Map[LookupExpression, Try[Iterable[AttributeValue]]]): Future[Stream[SubmissionValidationEntityInputs]] = ???

  override def expressionValidator: ExpressionValidator = ???

  override def batchUpdateEntities(entityUpdates: Seq[EntityUpdateDefinition]): Future[Traversable[Entity]] =
    throw new UnsupportedEntityOperationException("batch-update entities not supported by this provider.")

  override def batchUpsertEntities(entityUpdates: Seq[EntityUpdateDefinition]): Future[Traversable[Entity]] = {
    throw new UnsupportedEntityOperationException("batch-upsert entities not supported by this provider.")
  }

  // class-specific methods
  def createWorkspaceIndex(workspace: Workspace) = {
    val indexname = workspace.workspaceId

    val getIndexRequest = new GetIndexRequest(indexname)
    val getIndexResponse = client.indices().exists(getIndexRequest, RequestOptions.DEFAULT)
    if (!getIndexResponse) {
      logger.info(s"OpenSearch index for workspace ${workspace.workspaceId} (${workspace.toWorkspaceName.toString} does not exist; creating ... ")
      val createIndexRequest = new CreateIndexRequest(indexname)
      // TODO: explicit mappings
      val createIndexResponse = client.indices.create(createIndexRequest, RequestOptions.DEFAULT)
      logger.info(s"OpenSearch index for workspace ${workspace.workspaceId} (${workspace.toWorkspaceName.toString} created: ${createIndexResponse.index()}")
    } else {
      logger.info(s"OpenSearch index for workspace ${workspace.workspaceId} (${workspace.toWorkspaceName.toString} exists already; no action needed.")
    }
  }

  def indexEntity(workspace: Workspace, e: Entity) = {
    val indexname = workspace.workspaceId
    val request = new IndexRequest(indexname) //Add a document to the custom-index we created.

    request.id(s"${e.entityType}/${e.name}") //Assign an ID to the document.

    // need to unique-ify each attribute name, with entitytype
    val indexableAttrs = e.attributes.map {
      case (k, v) => (s"${e.entityType}.${AttributeName.toDelimitedName(k)}", v)
    }

    request.source(indexableAttrs.toJson.prettyPrint, XContentType.JSON) //Place your content into the index's source.

    val indexResponse = client.index(request, RequestOptions.DEFAULT)
    indexResponse.status().toString
  }



}
