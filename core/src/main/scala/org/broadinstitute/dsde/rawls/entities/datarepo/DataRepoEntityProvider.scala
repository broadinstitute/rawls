package org.broadinstitute.dsde.rawls.entities.datarepo

import java.util.UUID

import bio.terra.workspace.model.DataReferenceDescription.ReferenceTypeEnum
import com.typesafe.scalalogging.LazyLogging
import io.circe.Json
import io.circe.parser._
import org.broadinstitute.dsde.rawls.dataaccess.datarepo.{DataRepoDAO}
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.entities.base.EntityProvider
import org.broadinstitute.dsde.rawls.entities.exceptions.{DataEntityException, UnsupportedEntityOperationException}
import org.broadinstitute.dsde.rawls.model.{AttributeEntityReference, Entity, EntityTypeMetadata}

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

class DataRepoEntityProvider(requestArguments: EntityRequestArguments, workspaceManagerDAO: WorkspaceManagerDAO, dataRepoDAO: DataRepoDAO)
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
      // TODO: AS-321 better way to describe the primary key, if null or more than one PK?
      val primaryKey = Option(table.getPrimaryKey).getOrElse(new java.util.ArrayList()).asScala.mkString(",")
      // TODO: AS-321 once DR-1003 is implemented, add the actual row counts
      (table.getName, EntityTypeMetadata(-1, primaryKey, attrs))
    }.toMap

    Future.successful(entityTypesResponse)

  }

  override def createEntity(entity: Entity): Future[Entity] =
    throw new UnsupportedEntityOperationException("create entity not supported by this provider.")

  override def deleteEntities(entityRefs: Seq[AttributeEntityReference]): Future[Int] =
    throw new UnsupportedEntityOperationException("delete entities not supported by this provider.")

  // not marked as private to ease unit testing
  def lookupSnapshotForName(dataReferenceName: String): UUID = {
    // contact WSM to retrieve the data reference specified in the request
    val dataRef = workspaceManagerDAO.getDataReferenceByName(UUID.fromString(workspace.workspaceId),
      ReferenceTypeEnum.DATAREPOSNAPSHOT.getValue,
      dataReferenceName,
      userInfo.accessToken)
    // TODO: AS-321 verify we got one back (should be noop; request will throw if 0 found, but could use better error-trapping)

    // verify it's a TDR snapshot. should be a noop, since getDataReferenceByName enforces this.
    if (ReferenceTypeEnum.DATAREPOSNAPSHOT != dataRef.getReferenceType) {
      throw new DataEntityException(s"Reference type value for $dataReferenceName is not of type ${ReferenceTypeEnum.DATAREPOSNAPSHOT.getValue}")
    }

    // parse the reference value as a json object
    val refObj:Json = parse(dataRef.getReference) match {
      case Right(json) => json
      case Left(parsingFailure) => throw new DataEntityException(s"Could not parse reference value for $dataReferenceName: ${parsingFailure.message}", parsingFailure.underlying)
    }

    // verify reference object contains the instance and snapshotId keys
    // TODO: AS-321 should we parse into a case class instead of using Json directly?
    val cursor = refObj.hcursor
    val refInstance: String = cursor.get[String]("instance").getOrElse(throw new DataEntityException(s"Reference value for $dataReferenceName does not contain an instance value."))
    val refSnapshot: String = cursor.get[String]("snapshot").getOrElse(throw new DataEntityException(s"Reference value for $dataReferenceName does not contain a snapshot value."))

    // verify the instance matches our target instance
    // TODO: AS-321 is this the right place to validate this? We could add a "validateInstanceURL" method to the DAO itself, for instance
    if (refInstance != dataRepoDAO.getBaseURL) {
      logger.error(s"expected instance ${dataRepoDAO.getBaseURL}, got $refInstance")
      throw new DataEntityException(s"Reference value for $dataReferenceName contains an unexpected instance value")
    }

    // verify snapshotId value is a UUID
    Try(UUID.fromString(refSnapshot)) match {
      case Success(uuid) => uuid
      case Failure(ex) =>
        logger.error(s"invalid UUID for snapshotId in reference: $refSnapshot")
        throw new DataEntityException(s"Reference value for $dataReferenceName contains an unexpected snapshot value", ex)
    }

  }



}
