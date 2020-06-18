package org.broadinstitute.dsde.rawls.entities.datarepo

import java.util.UUID

import bio.terra.datarepo.model.TableModel
import bio.terra.workspace.model.DataReferenceDescription.ReferenceTypeEnum
import cats.effect.{IO, Resource}
import com.google.cloud.bigquery.{QueryJobConfiguration, TableResult}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.datarepo.DataRepoDAO
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleBigQueryServiceFactory, SamDAO}
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.entities.base.{EntityProvider, ExpressionEvaluationContext, ExpressionValidator}
import org.broadinstitute.dsde.rawls.entities.exceptions.{DataEntityException, EntityTypeNotFoundException, UnsupportedEntityOperationException}
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.GatherInputsResult
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model.DataReferenceModelJsonSupport.TerraDataRepoSnapshotRequestFormat
import org.broadinstitute.dsde.rawls.model.{AttributeEntityReference, AttributeName, AttributeString, Entity, EntityTypeMetadata, SubmissionValidationEntityInputs, TerraDataRepoSnapshotRequest}
import spray.json._

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class DataRepoEntityProvider(requestArguments: EntityRequestArguments, workspaceManagerDAO: WorkspaceManagerDAO,
                             dataRepoDAO: DataRepoDAO, samDAO: SamDAO, bqServiceFactory: GoogleBigQueryServiceFactory[IO])
                            (implicit protected val executionContext: ExecutionContext)
  extends EntityProvider with LazyLogging {

  val workspace = requestArguments.workspace
  val userInfo = requestArguments.userInfo
  val dataReferenceName = requestArguments.dataReference.getOrElse(throw new DataEntityException("data reference must be defined for this provider"))

  override def entityTypeMetadata(): Future[Map[String, EntityTypeMetadata]] = {

    // TODO: AS-321 auto-switch to see if the ref supplied in argument is a UUID or a name?? Use separate query params? Never allow ID?

    // get snapshotId from reference name
    val snapshotId = lookupSnapshotForName(dataReferenceName)

    // contact TDR to describe the snapshot
    val snapshotModel = dataRepoDAO.getSnapshot(snapshotId, userInfo.accessToken)

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
    // get snapshot UUID from data reference name
    val snapshotId = lookupSnapshotForName(dataReferenceName)

    // contact TDR to describe the snapshot
    val snapshotModel = dataRepoDAO.getSnapshot(snapshotId, userInfo.accessToken)

    // extract table definition, with PK, from snapshot schema
    val tableModel = snapshotModel.getTables.asScala.find(_.getName == entityType) match {
      case Some(table) => table
      case None => throw new EntityTypeNotFoundException(entityType)
    }

    // determine project to be billed for the BQ job TODO: need business logic from PO!
    val googleProject: String = requestArguments.billingProject match {
      case Some(billing) => billing.projectName.value
      case None => workspace.namespace
    }

    // generate BQ SQL for this entity

    //  determine pk column
    val pk = pkFromSnapshotTable(tableModel)
    // determine data project
    val dataProject = snapshotModel.getDataProject
    // determine view name
    val viewName = snapshotModel.getName

    // TODO: use named params!
    val query = s"SELECT * FROM `${dataProject}.${viewName}.${entityType}` WHERE $pk = '$entityName';"

    val queryConfig = QueryJobConfiguration.newBuilder(query).build

    // get pet service account key for this user
    samDAO.getPetServiceAccountKeyForUser(googleProject, requestArguments.userInfo.userEmail) map { petKey =>

      val foo: Resource[IO, TableResult] = for {
        bqService <- bqServiceFactory.getServiceForPet(petKey)
        queryResults <- Resource.liftF(bqService.query(queryConfig))
      } yield queryResults

      foo.use {
        case queryResults:TableResult =>
          queryResults.getTotalRows match {
            case 1 =>
              val fieldValues = queryResults.iterateAll().asScala.head

              val fieldNames:List[String] = queryResults.getSchema.getFields.iterator().asScala.toList.map(_.getName)

              val attrs:AttributeMap = fieldNames.map { fieldName =>
                val fv = fieldValues.get(fieldName)
                AttributeName.withDefaultNS(fieldName) -> AttributeString(fv.getValue.toString)
              }.toMap

              IO.pure(Entity(entityName, entityType, attrs))
            case 0 =>
              throw new DataEntityException("BQ succeeded, but returned zero rows") // error not found
            case _ =>
              throw new DataEntityException(s"BQ succeeded, but returned ${queryResults.getTotalRows} rows") // unexpected error: too many found
          }
      }.unsafeRunSync()

    }

  }

  // not marked as private to ease unit testing
  def lookupSnapshotForName(dataReferenceName: String): UUID = {
    // contact WSM to retrieve the data reference specified in the request
    val dataRef = workspaceManagerDAO.getDataReferenceByName(UUID.fromString(workspace.workspaceId),
      ReferenceTypeEnum.DATAREPOSNAPSHOT.getValue,
      dataReferenceName,
      userInfo.accessToken)

    // the above request will throw a 404 if the reference is not found, so we can assume we have one by the time we reach here.

    // verify it's a TDR snapshot. should be a noop, since getDataReferenceByName enforces this.
    if (ReferenceTypeEnum.DATAREPOSNAPSHOT != dataRef.getReferenceType) {
      throw new DataEntityException(s"Reference type value for $dataReferenceName is not of type ${ReferenceTypeEnum.DATAREPOSNAPSHOT.getValue}")
    }

    // parse the raw reference value into a snapshot reference
    val dataReference = Try(dataRef.getReference.parseJson.convertTo[TerraDataRepoSnapshotRequest]) match {
      case Success(ref) => ref
      case Failure(err) => throw new DataEntityException(s"Could not parse reference value for $dataReferenceName: ${err.getMessage}", err)
    }

    // verify the instance matches our target instance
    // TODO: AS-321 is this the right place to validate this? We could add a "validateInstanceURL" method to the DAO itself, for instance
    if (!dataReference.instanceName.equalsIgnoreCase(dataRepoDAO.getInstanceName)) {
      logger.error(s"expected instance name ${dataRepoDAO.getInstanceName}, got ${dataReference.instanceName}")
      throw new DataEntityException(s"Reference value for $dataReferenceName contains an unexpected instance name value")
    }

    // verify snapshotId value is a UUID
    Try(UUID.fromString(dataReference.snapshot)) match {
      case Success(uuid) => uuid
      case Failure(ex) =>
        logger.error(s"invalid UUID for snapshotId in reference: ${dataReference.snapshot}")
        throw new DataEntityException(s"Reference value for $dataReferenceName contains an unexpected snapshot value", ex)
    }

  }

  private def pkFromSnapshotTable(tableModel: TableModel): String = {
    // If data repo returns one and only one primary key, use it.
    // If data repo returns null or a compound PK, use the built-in rowid for pk instead.
    Option(tableModel.getPrimaryKey) match {
      case Some(pk) if pk.size() == 1 => pk.asScala.head
      case _ => "datarepo_row_id" // default data repo value
    }
  }


  override def evaluateExpressions(expressionEvaluationContext: ExpressionEvaluationContext, gatherInputsResult: GatherInputsResult): Future[Stream[SubmissionValidationEntityInputs]] =
    throw new UnsupportedEntityOperationException("evaluateExpressions not supported by this provider.")

  override def expressionValidator: ExpressionValidator =
    throw new UnsupportedEntityOperationException("expressionEvaluator not supported by this provider.")



}
