package org.broadinstitute.dsde.rawls.entities.datarepo

import akka.http.scaladsl.model.StatusCodes
import bio.terra.datarepo.model.{SnapshotModel, TableModel}
import cats.effect.{IO, Resource}
import com.google.cloud.bigquery.{QueryJobConfiguration, QueryParameterValue, TableResult}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleBigQueryServiceFactory, SamDAO}
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.entities.base.{EntityProvider, ExpressionEvaluationContext, ExpressionValidator}
import org.broadinstitute.dsde.rawls.entities.exceptions.{DataEntityException, EntityTypeNotFoundException, UnsupportedEntityOperationException}
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.GatherInputsResult
import org.broadinstitute.dsde.rawls.model.{AttributeEntityReference, Entity, EntityQuery, EntityQueryResponse, EntityTypeMetadata, SubmissionValidationEntityInputs}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

class DataRepoEntityProvider(snapshotModel: SnapshotModel, requestArguments: EntityRequestArguments,
                             samDAO: SamDAO, bqServiceFactory: GoogleBigQueryServiceFactory)
                            (implicit protected val executionContext: ExecutionContext)
  extends EntityProvider with DataRepoBigQuerySupport with LazyLogging {

  override def entityTypeMetadata(): Future[Map[String, EntityTypeMetadata]] = {

    // TODO: AS-321 auto-switch to see if the ref supplied in argument is a UUID or a name?? Use separate query params? Never allow ID?

    // reformat TDR's response into the expected response structure
    val entityTypesResponse: Map[String, EntityTypeMetadata] = snapshotModel.getTables.asScala.map { table =>
      val attrs: Seq[String] = table.getColumns.asScala.map(_.getName)
      val primaryKey = pkFromSnapshotTable(table)
      (table.getName, EntityTypeMetadata(table.getRowCount, primaryKey, attrs))
    }.toMap

    Future.successful(entityTypesResponse)

  }

  override def createEntity(entity: Entity): Future[Entity] =
    throw new UnsupportedEntityOperationException("create entity not supported by this provider.")

  override def deleteEntities(entityRefs: Seq[AttributeEntityReference]): Future[Int] =
    throw new UnsupportedEntityOperationException("delete entities not supported by this provider.")


  override def getEntity(entityType: String, entityName: String): Future[Entity] = {
    // extract table definition, with PK, from snapshot schema
    val tableModel = snapshotModel.getTables.asScala.find(_.getName == entityType) match {
      case Some(table) => table
      case None => throw new EntityTypeNotFoundException(entityType)
    }

    // determine project to be billed for the BQ job TODO: need business logic from PO!
    val googleProject: String = requestArguments.billingProject match {
      case Some(billing) => billing.projectName.value
      case None => requestArguments.workspace.namespace
    }

    //  determine pk column
    val pk = pkFromSnapshotTable(tableModel)
    // determine data project
    val dataProject = snapshotModel.getDataProject
    // determine view name
    val viewName = snapshotModel.getName
    // generate BQ SQL for this entity
    // TODO: prevent injection via dataProject, viewName, entityType vars. These are calculated from the snapshot so
    // they should be safe, but we should have layers of protection.
    val query = s"SELECT * FROM `${sanitizeSql(dataProject)}.${sanitizeSql(viewName)}.${sanitizeSql(entityType)}` WHERE $pk = @pkvalue;"
    // generate query config, with named param for primary key
    val queryConfig = QueryJobConfiguration.newBuilder(query)
      .addNamedParameter("pkvalue", QueryParameterValue.string(entityName))
      .build

    // get pet service account key for this user
    samDAO.getPetServiceAccountKeyForUser(googleProject, requestArguments.userInfo.userEmail) map { petKey =>

      // get a BQ service (i.e. dao) instance, and use it to execute the query against BQ
      val queryResource: Resource[IO, TableResult] = for {
        bqService <- bqServiceFactory.getServiceForPet(petKey)
        queryResults <- Resource.liftF(bqService.query(queryConfig))
      } yield queryResults

      // translate the BQ results into a single Rawls Entity
      queryResource.use { queryResults: TableResult =>
        IO.pure(queryResultsToEntity(queryResults, entityType, pk))
      }.unsafeRunSync()

    }
  }

  override def queryEntities(entityType: String, entityQuery: EntityQuery): Future[EntityQueryResponse] = {
    // throw immediate error if user supplied filterTerms
    if (entityQuery.filterTerms.nonEmpty) {
      throw new UnsupportedEntityOperationException("term filtering not supported by this provider.")
    }

    // extract table definition, with PK, from snapshot schema
    val tableModel = snapshotModel.getTables.asScala.find(_.getName == entityType) match {
      case Some(table) => table
      case None => throw new EntityTypeNotFoundException(entityType)
    }

    // determine project to be billed for the BQ job TODO: need business logic from PO!
    val googleProject: String = requestArguments.billingProject match {
      case Some(billing) => billing.projectName.value
      case None => requestArguments.workspace.namespace
    }

    // validate sort column exists in the snapshot's table description
    if (!tableModel.getColumns.asScala.exists(_.getName == entityQuery.sortField))
      throw new DataEntityException(code = StatusCodes.BadRequest, message = s"sortField not valid for this entity type")

    //  determine pk column
    val pk = pkFromSnapshotTable(tableModel)
    // determine data project
    val dataProject = snapshotModel.getDataProject
    // determine view name
    val viewName = snapshotModel.getName

    val queryConfig = queryConfigForQueryEntities(dataProject, viewName, entityType, entityQuery)

    // get pet service account key for this user
    samDAO.getPetServiceAccountKeyForUser(googleProject, requestArguments.userInfo.userEmail) map { petKey =>

      // get a BQ service (i.e. dao) instance, and use it to execute the query against BQ
      val queryResource: Resource[IO, TableResult] = for {
        bqService <- bqServiceFactory.getServiceForPet(petKey)
        queryResults <- Resource.liftF(bqService.query(queryConfig))
      } yield queryResults

      // translate the BQ results into a Rawls query result
      queryResource.use { queryResults: TableResult =>
        val page = queryResultsToEntities(queryResults, entityType, pk)
        val metadata = queryResultsMetadata(queryResults, entityQuery)
        val queryResponse = EntityQueryResponse(entityQuery, metadata, page)
        IO.pure(queryResponse)
      }.unsafeRunSync()

    }
  }

  def pkFromSnapshotTable(tableModel: TableModel): String = {
    // If data repo returns one and only one primary key, use it.
    // If data repo returns null or a compound PK, use the built-in rowid for pk instead.
    scala.Option(tableModel.getPrimaryKey) match {
      case Some(pk) if pk.size() == 1 => pk.asScala.head
      case _ => "datarepo_row_id" // default data repo value
    }
  }

  override def evaluateExpressions(expressionEvaluationContext: ExpressionEvaluationContext, gatherInputsResult: GatherInputsResult): Future[Stream[SubmissionValidationEntityInputs]] =
    throw new UnsupportedEntityOperationException("evaluateExpressions not supported by this provider.")

  override def expressionValidator: ExpressionValidator = new DataRepoEntityExpressionValidator(snapshotModel)

}
