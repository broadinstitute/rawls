package org.broadinstitute.dsde.rawls.entities.datarepo

import java.util.UUID

import akka.http.scaladsl.model.StatusCodes
import bio.terra.datarepo.client.{ApiException => DatarepoApiException}
import bio.terra.workspace.client.{ApiException => WorkspaceApiException}
import bio.terra.workspace.model.DataReferenceDescription.ReferenceTypeEnum
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.config.DataRepoEntityProviderConfig
import org.broadinstitute.dsde.rawls.dataaccess.datarepo.DataRepoDAO
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleBigQueryServiceFactory, SamDAO}
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.entities.base.EntityProviderBuilder
import org.broadinstitute.dsde.rawls.entities.exceptions.DataEntityException
import org.broadinstitute.dsde.rawls.model.DataReferenceModelJsonSupport.TerraDataRepoSnapshotRequestFormat
import org.broadinstitute.dsde.rawls.model.{DataReferenceName, TerraDataRepoSnapshotRequest}
import spray.json._

import scala.concurrent.ExecutionContext
import scala.reflect.runtime.universe._
import scala.util.{Failure, Success, Try}

class DataRepoEntityProviderBuilder(workspaceManagerDAO: WorkspaceManagerDAO, dataRepoDAO: DataRepoDAO,
                                    samDAO: SamDAO, bqServiceFactory: GoogleBigQueryServiceFactory,
                                    config: DataRepoEntityProviderConfig)
                                   (implicit protected val executionContext: ExecutionContext)
  extends EntityProviderBuilder[DataRepoEntityProvider] with LazyLogging {

  override def builds: TypeTag[DataRepoEntityProvider] = typeTag[DataRepoEntityProvider]

  override def build(requestArguments: EntityRequestArguments): Try[DataRepoEntityProvider] = {
    for {
      dataReferenceName <- requestArguments.dataReference.toRight(new DataEntityException("data reference must be defined for this provider")).toTry

      // get snapshot UUID from data reference name
      snapshotId <- Try(lookupSnapshotForName(dataReferenceName, requestArguments))

      // contact TDR to describe the snapshot
      snapshotModel <- Try(dataRepoDAO.getSnapshot(snapshotId, requestArguments.userInfo.accessToken)).recoverWith {
        case notFound: DatarepoApiException if notFound.getCode == StatusCodes.NotFound.intValue =>
          Failure(new DataEntityException(s"Snapshot id $snapshotId does not exist or you do not have access", notFound))
        case forbidden: DatarepoApiException if forbidden.getCode == StatusCodes.Forbidden.intValue =>
          Failure(new DataEntityException(s"Snapshot id $snapshotId exists but access was denied", forbidden))
      }
    } yield new DataRepoEntityProvider(snapshotModel, requestArguments, samDAO, bqServiceFactory, config)
  }

  private[datarepo] def lookupSnapshotForName(dataReferenceName: DataReferenceName, requestArguments: EntityRequestArguments): UUID = {
    // contact WSM to retrieve the data reference specified in the request
    val dataRefTry = Try(workspaceManagerDAO.getDataReferenceByName(UUID.fromString(requestArguments.workspace.workspaceId),
      ReferenceTypeEnum.DATAREPOSNAPSHOT.getValue,
      dataReferenceName,
      requestArguments.userInfo.accessToken)).recoverWith {

      case notFound: WorkspaceApiException if notFound.getCode == StatusCodes.NotFound.intValue =>
        Failure(new DataEntityException(s"Reference name ${dataReferenceName.value} does not exist in workspace ${requestArguments.workspace.toWorkspaceName}.", notFound))
      case forbidden: WorkspaceApiException if forbidden.getCode == StatusCodes.Forbidden.intValue =>
        Failure(new DataEntityException(s"Access denied for reference name ${dataReferenceName.value}.", forbidden))
    }

    // trigger any exceptions
    val dataRef = dataRefTry.get

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
}