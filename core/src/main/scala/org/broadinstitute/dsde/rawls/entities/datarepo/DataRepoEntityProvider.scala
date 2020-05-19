package org.broadinstitute.dsde.rawls.entities.datarepo

import java.util.UUID

import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.entities.base.EntityProvider
import org.broadinstitute.dsde.rawls.entities.exceptions.UnsupportedEntityOperationException
import org.broadinstitute.dsde.rawls.model.{AttributeEntityReference, Entity, EntityTypeMetadata}

import scala.concurrent.Future

class DataRepoEntityProvider(requestArguments: EntityRequestArguments, workspaceManagerDAO: WorkspaceManagerDAO, terraDataRepoUrl: String) extends EntityProvider {

  val workspace = requestArguments.workspace
  val userInfo = requestArguments.userInfo
  val foo = requestArguments.dataReference.get // TODO: or else throw, as part of validation

  override def entityTypeMetadata(): Future[Map[String, EntityTypeMetadata]] = {

    // TODO: auto-switch to see if the ref supplied in argument is a UUID or a name??

    // TODO: contact WSM to retrieve the data reference specified in the request
    val dataRef = workspaceManagerDAO.getDataReference(UUID.fromString(workspace.workspaceId),
      UUID.fromString(requestArguments.dataReference.get), userInfo.accessToken)
    // TODO: verify we got one back (should be noop; request will throw if 0 found)

    // TODO: extract the TDR snapshot ID from the WSM response
    dataRef.getReferenceType // verify it's a TDR snapshot.
    dataRef.getReference // should be snapshotid, verify it is a UUID; TODO: is this the right field to look in?

    // TODO: contact TDR to describe the snapshot
    // TODO: reformat TDR's response into the expected response structure
    throw new UnsupportedEntityOperationException("type metadata will be supported by this provider, but is not implemented yet")
  }

  override def createEntity(entity: Entity): Future[Entity] =
    throw new UnsupportedEntityOperationException("create entity not supported by this provider.")

  override def deleteEntities(entityRefs: Seq[AttributeEntityReference]): Future[Int] =
    throw new UnsupportedEntityOperationException("delete entities not supported by this provider.")
}
