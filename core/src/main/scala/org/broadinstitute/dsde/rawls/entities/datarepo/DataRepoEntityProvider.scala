package org.broadinstitute.dsde.rawls.entities.datarepo

import java.util.UUID

import bio.terra.workspace.model.DataReferenceDescription.ReferenceTypeEnum
import com.typesafe.scalalogging.LazyLogging
import io.circe.Json
import io.circe.parser._
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.entities.base.EntityProvider
import org.broadinstitute.dsde.rawls.entities.exceptions.{DataEntityException, UnsupportedEntityOperationException}
import org.broadinstitute.dsde.rawls.model.{AttributeEntityReference, Entity, EntityTypeMetadata}

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

class DataRepoEntityProvider(requestArguments: EntityRequestArguments, workspaceManagerDAO: WorkspaceManagerDAO, terraDataRepoUrl: String)
  extends EntityProvider with LazyLogging {

  val workspace = requestArguments.workspace
  val userInfo = requestArguments.userInfo
  val dataReferenceName = requestArguments.dataReference.getOrElse(throw new DataEntityException("data reference must be defined for this provider"))

  override def entityTypeMetadata(): Future[Map[String, EntityTypeMetadata]] = {

    // TODO: auto-switch to see if the ref supplied in argument is a UUID or a name??

    // get snapshotId from reference name
    val snapshotId = lookupSnapshotForName(dataReferenceName)

    Future.successful(Map((snapshotId.toString, EntityTypeMetadata(-1, "snapshotId", Seq.empty[String]))))

    // TODO: contact TDR to describe the snapshot
    // TODO: reformat TDR's response into the expected response structure
  }

  override def createEntity(entity: Entity): Future[Entity] =
    throw new UnsupportedEntityOperationException("create entity not supported by this provider.")

  override def deleteEntities(entityRefs: Seq[AttributeEntityReference]): Future[Int] =
    throw new UnsupportedEntityOperationException("delete entities not supported by this provider.")

  private def lookupSnapshotForName(dataReferenceName: String): UUID = {
    // contact WSM to retrieve the data reference specified in the request
    // TODO: this should use lookup-by-name, not lookup-by-id
    val dataRef = workspaceManagerDAO.getDataReference(UUID.fromString(workspace.workspaceId),
      UUID.fromString(dataReferenceName), userInfo.accessToken)
    // TODO: verify we got one back (should be noop; request will throw if 0 found)

    // verify it's a TDR snapshot.
    if (ReferenceTypeEnum.DATAREPOSNAPSHOT != dataRef.getReferenceType) {
      throw new DataEntityException(s"Reference value for $dataReferenceName is not of type ${ReferenceTypeEnum.DATAREPOSNAPSHOT.getValue}")
    }

    // parse the reference value as a json object
    val refObj:Json = parse(dataRef.getReference) match {
      case Right(json) => json
      case Left(parsingFailure) => throw new DataEntityException(s"Could not parse reference value for $dataReferenceName: ${parsingFailure.message}", parsingFailure.underlying)
    }

    // verify reference object contains the instance and snapshotId keys
    val cursor = refObj.hcursor
    val refInstance: String = cursor.get[String]("instance").getOrElse(throw new DataEntityException(s"Reference value for $dataReferenceName does not contain an instance value."))
    val refSnapshot: String = cursor.get[String]("snapshot").getOrElse(throw new DataEntityException(s"Reference value for $dataReferenceName does not contain a snapshotId value."))

    // verify the instance matches our target instance
    if (refInstance != terraDataRepoUrl) {
      logger.error(s"expected instance $terraDataRepoUrl, got $refInstance")
      throw new DataEntityException(s"Reference value for $dataReferenceName contains an unexpected instance value")
    }

    // verify snapshotId value is a UUID
    Try(UUID.fromString(refSnapshot)) match {
      case Success(uuid) => uuid
      case Failure(ex) =>
        logger.error(s"invalid UUID for snapshotId in reference: $refSnapshot")
        throw new DataEntityException(s"Reference value for $dataReferenceName contains an unexpected snapshotId value", ex)
    }

  }



}
