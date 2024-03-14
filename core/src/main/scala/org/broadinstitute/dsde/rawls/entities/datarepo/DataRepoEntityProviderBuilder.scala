package org.broadinstitute.dsde.rawls.entities.datarepo

import akka.http.scaladsl.model.StatusCodes
import bio.terra.datarepo.client.{ApiException => DatarepoApiException}
import bio.terra.workspace.client.{ApiException => WorkspaceApiException}
import bio.terra.workspace.model.{DataRepoSnapshotResource, ResourceType}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.config.DataRepoEntityProviderConfig
import org.broadinstitute.dsde.rawls.dataaccess.datarepo.DataRepoDAO
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleBigQueryServiceFactory, GoogleBigQueryServiceFactoryImpl, SamDAO}
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.entities.base.EntityProviderBuilder
import org.broadinstitute.dsde.rawls.entities.exceptions.DataEntityException
import org.broadinstitute.dsde.rawls.model.DataReferenceName

import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.reflect.runtime.universe._
import scala.util.{Failure, Success, Try}

class DataRepoEntityProviderBuilder(workspaceManagerDAO: WorkspaceManagerDAO,
                                    dataRepoDAO: DataRepoDAO,
                                    samDAO: SamDAO,
                                    bqServiceFactory: GoogleBigQueryServiceFactory,
                                    config: DataRepoEntityProviderConfig
)(implicit protected val executionContext: ExecutionContext)
    extends EntityProviderBuilder[DataRepoEntityProvider]
    with LazyLogging {

  override def builds: TypeTag[DataRepoEntityProvider] = typeTag[DataRepoEntityProvider]

  override def build(requestArguments: EntityRequestArguments): Try[DataRepoEntityProvider] =
    for {
      dataReferenceName <- requestArguments.dataReference
        .toRight(new DataEntityException("data reference must be defined for this provider"))
        .toTry

      // get snapshot UUID from data reference name
      dataReference <- Try(lookupSnapshotForName(dataReferenceName, requestArguments))
      snapshotId = UUID.fromString(dataReference.getAttributes.getSnapshot)

      // contact TDR to describe the snapshot
      snapshotModel <- Try(dataRepoDAO.getSnapshot(snapshotId, requestArguments.ctx.userInfo.accessToken)).recoverWith {
        case notFound: DatarepoApiException if notFound.getCode == StatusCodes.NotFound.intValue =>
          Failure(
            new DataEntityException(s"Snapshot id $snapshotId does not exist or you do not have access", notFound)
          )

        // Data Repo returns 401 Unauthorized if the user has a valid token but does not have the proper permission on the snapshot
        case forbidden: DatarepoApiException
            if forbidden.getCode == StatusCodes.Forbidden.intValue || forbidden.getCode == StatusCodes.Unauthorized.intValue =>
          // attempt to extract buried message
          // raw response looks like: {"message":"User 'davidan.dev2@gmail.com' does not have required action: read_data","errorDetail":[]}
          import spray.json._
          val innerErrorMessage: String =
            Try(forbidden.getMessage.parseJson.asJsObject.fields.get("message")).toOption.flatten
              .collect { case jss: JsString =>
                jss.value
              }
              .getOrElse("[error could not be deserialized]")

          val finalErrMessage = s"Snapshot id $snapshotId exists but access was denied: $innerErrorMessage"

          // log the full error, but don't include giant stack traces in the failure we bubble up to the end user
          logger.warn(finalErrMessage, forbidden)
          Failure(new DataEntityException(code = StatusCodes.Forbidden, message = finalErrMessage))
      }
    } yield new DataRepoEntityProvider(snapshotModel, requestArguments, samDAO, bqServiceFactory, config)

  private[datarepo] def lookupSnapshotForName(dataReferenceName: DataReferenceName,
                                              requestArguments: EntityRequestArguments
  ): DataRepoSnapshotResource = {
    // contact WSM to retrieve the data reference specified in the request
    val dataRefTry = Try(
      workspaceManagerDAO.getDataRepoSnapshotReferenceByName(UUID.fromString(requestArguments.workspace.workspaceId),
                                                             dataReferenceName,
                                                             requestArguments.ctx
      )
    ).recoverWith {

      case notFound: WorkspaceApiException if notFound.getCode == StatusCodes.NotFound.intValue =>
        Failure(
          new DataEntityException(
            s"Reference name ${dataReferenceName.value} does not exist in workspace ${requestArguments.workspace.toWorkspaceName}.",
            notFound,
            StatusCodes.NotFound
          )
        )
      case forbidden: WorkspaceApiException if forbidden.getCode == StatusCodes.Forbidden.intValue =>
        Failure(
          new DataEntityException(s"Access denied for reference name ${dataReferenceName.value}.",
                                  forbidden,
                                  StatusCodes.Forbidden
          )
        )
    }

    // trigger any exceptions
    val dataRef = dataRefTry.get

    // ultimately this method will return dataRef, but we'll validate its contents before returning it

    // verify it's a TDR snapshot. should be a noop, since getDataReferenceByName enforces this.
    if (ResourceType.DATA_REPO_SNAPSHOT != dataRef.getMetadata.getResourceType) {
      throw new DataEntityException(
        s"Reference type value for $dataReferenceName is not of type ${ResourceType.DATA_REPO_SNAPSHOT.getValue}"
      )
    }

    val dataReference = dataRef.getAttributes

    // verify the instance matches our target instance
    // TODO: AS-321 is this the right place to validate this? We could add a "validateInstanceURL" method to the DAO itself, for instance
    if (!dataReference.getInstanceName.equalsIgnoreCase(dataRepoDAO.getInstanceName)) {
      logger.error(s"expected instance name ${dataRepoDAO.getInstanceName}, got ${dataReference.getInstanceName}")
      throw new DataEntityException(
        s"Reference value for $dataReferenceName contains an unexpected instance name value"
      )
    }

    // verify snapshotId value is a UUID
    Try(UUID.fromString(dataReference.getSnapshot)) match {
      case Success(uuid) => // success; noop
      case Failure(ex) =>
        logger.error(s"invalid UUID for snapshotId in reference: ${dataReference.getSnapshot}")
        throw new DataEntityException(s"Reference value for $dataReferenceName contains an unexpected snapshot value",
                                      ex
        )
    }

    dataRef
  }
}
