package org.broadinstitute.dsde.rawls.webservice

import java.util.UUID

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import spray.json.DefaultJsonProtocol._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import bio.terra.workspace.model.{DataReferenceDescription, DataReferenceList, DataRepoSnapshot, DataRepoSnapshotResource, ReferenceTypeEnum, ResourceList, UpdateDataReferenceRequestBody}
import org.broadinstitute.dsde.rawls.model.DataReferenceModelJsonSupport._
import org.broadinstitute.dsde.rawls.model.{NamedDataRepoSnapshot, SnapshotListResponse, UserInfo, WorkspaceName}
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.snapshot.SnapshotService

import scala.concurrent.ExecutionContext

trait SnapshotApiService extends UserInfoDirectives {

  implicit val executionContext: ExecutionContext

  val snapshotServiceConstructor: UserInfo => SnapshotService

  val snapshotRoutes: server.Route = requireUserInfo() { userInfo =>
    path("workspaces" /  Segment / Segment / "snapshots" / "v2") { (workspaceNamespace, workspaceName) =>
      post {
        entity(as[NamedDataRepoSnapshot]) { namedDataRepoSnapshot =>
          complete {
            snapshotServiceConstructor(userInfo).createSnapshot(WorkspaceName(workspaceNamespace, workspaceName), namedDataRepoSnapshot).map(StatusCodes.Created -> _)
          }
        }
      } ~
      get {
        // N.B. the "as[UUID]" delegates to SnapshotService.validateSnapshotId, which is in scope;
        // that method provides a 400 Bad Request response and nice error message
        parameters("offset".as[Int], "limit".as[Int], "referencedSnapshotId".as[UUID].optional) { (offset, limit, referencedSnapshotId) =>
          complete {
            snapshotServiceConstructor(userInfo).enumerateSnapshots(WorkspaceName(workspaceNamespace, workspaceName), offset, limit, referencedSnapshotId).map(StatusCodes.OK -> _)
          }
        }
      }
    } ~
    path("workspaces" / Segment / Segment / "snapshots" / "v2" / Segment) { (workspaceNamespace, workspaceName, snapshotId) =>
      get {
        complete {
          snapshotServiceConstructor(userInfo).getSnapshot(WorkspaceName(workspaceNamespace, workspaceName), snapshotId).map(StatusCodes.OK -> _)
        }
      } ~
      patch {
        entity(as[UpdateDataReferenceRequestBody]) { updateDataReferenceRequestBody =>
          complete {
            snapshotServiceConstructor(userInfo).updateSnapshot(WorkspaceName(workspaceNamespace, workspaceName), snapshotId, updateDataReferenceRequestBody).map(_ => StatusCodes.NoContent)
          }
        }
      } ~
      delete {
        complete {
          snapshotServiceConstructor(userInfo).deleteSnapshot(WorkspaceName(workspaceNamespace, workspaceName), snapshotId).map(_ => StatusCodes.NoContent)
        }
      }
    } ~
    path("workspaces" / Segment / Segment / "snapshots" / "v2" / "name" / Segment) { (workspaceNamespace, workspaceName, referenceName) =>
      complete {
        snapshotServiceConstructor(userInfo).getSnapshotByName(WorkspaceName(workspaceNamespace, workspaceName), referenceName).map(StatusCodes.OK -> _)
      }
    } ~
    // ---- SNAPSHOT V1 ----
    path("workspaces" / Segment / Segment / "snapshots") { (workspaceNamespace, workspaceName) =>
      post {
        entity(as[NamedDataRepoSnapshot]) { namedDataRepoSnapshot =>
          complete {
            snapshotServiceConstructor(userInfo).createSnapshot(WorkspaceName(workspaceNamespace, workspaceName), namedDataRepoSnapshot)
              .map(dataRepoSnapshotResourceToDataReferenceDescription)
              .map(StatusCodes.Created -> _)
          }
        }
      } ~
      get {
        parameters("offset".as[Int], "limit".as[Int]) { (offset, limit) =>
          complete {
            snapshotServiceConstructor(userInfo).enumerateSnapshots(WorkspaceName(workspaceNamespace, workspaceName), offset, limit)
              .map(snapshotListResponseToDataReferenceList)
              .map(StatusCodes.OK -> _)
          }
        }
      }
    } ~
    path("workspaces" / Segment / Segment / "snapshots" / "v2" / Segment) { (workspaceNamespace, workspaceName, snapshotId) =>
      get {
        complete {
          snapshotServiceConstructor(userInfo).getSnapshot(WorkspaceName(workspaceNamespace, workspaceName), snapshotId).map(StatusCodes.OK -> _)
        }
      } ~
      patch {
        entity(as[UpdateDataReferenceRequestBody]) { updateDataReferenceRequestBody =>
          complete {
            snapshotServiceConstructor(userInfo).updateSnapshot(WorkspaceName(workspaceNamespace, workspaceName), snapshotId, updateDataReferenceRequestBody).map(_ => StatusCodes.NoContent)
          }
        }
      } ~
      delete {
        complete {
          snapshotServiceConstructor(userInfo).deleteSnapshot(WorkspaceName(workspaceNamespace, workspaceName), snapshotId).map(_ => StatusCodes.NoContent)
        }
      }
    } ~
    // ---- SNAPSHOT V1 ----
    path("workspaces" / Segment / Segment / "snapshots") { (workspaceNamespace, workspaceName) =>
      post {
        entity(as[NamedDataRepoSnapshot]) { namedDataRepoSnapshot =>
          complete {
            snapshotServiceConstructor(userInfo).createSnapshot(WorkspaceName(workspaceNamespace, workspaceName), namedDataRepoSnapshot)
              .map(dataRepoSnapshotResourceToDataReferenceDescription)
              .map(StatusCodes.Created -> _)
          }
        }
      } ~
      get {
        parameters("offset".as[Int], "limit".as[Int]) { (offset, limit) =>
          complete {
            snapshotServiceConstructor(userInfo).enumerateSnapshots(WorkspaceName(workspaceNamespace, workspaceName), offset, limit)
              .map(snapshotListResponseToDataReferenceList)
              .map(StatusCodes.OK -> _)
          }
        }
      }
    } ~
    path("workspaces" / Segment / Segment / "snapshots" / Segment) { (workspaceNamespace, workspaceName, snapshotId) =>
      get {
        complete {
          snapshotServiceConstructor(userInfo).getSnapshot(WorkspaceName(workspaceNamespace, workspaceName), snapshotId)
            .map(dataRepoSnapshotResourceToDataReferenceDescription)
            .map(StatusCodes.OK -> _)
        }
      } ~
      patch {
        entity(as[UpdateDataReferenceRequestBody]) { updateDataReferenceRequestBody =>
          complete {
            snapshotServiceConstructor(userInfo).updateSnapshot(WorkspaceName(workspaceNamespace, workspaceName), snapshotId, updateDataReferenceRequestBody).map(_ => StatusCodes.NoContent)
          }
        }
      } ~
      delete {
        complete {
          snapshotServiceConstructor(userInfo).deleteSnapshot(WorkspaceName(workspaceNamespace, workspaceName), snapshotId).map(_ => StatusCodes.NoContent)
        }
      }
    }
  }

  // -------  REMOVE WHEN GETTING RID OF V1 SNAPSHOT ENDPOINTS -------

  private def dataRepoSnapshotResourceToDataReferenceDescription(resource: DataRepoSnapshotResource): DataReferenceDescription = {
    new DataReferenceDescription()
      .name(resource.getMetadata.getName)
      .description(resource.getMetadata.getDescription)
      .referenceId(resource.getMetadata.getResourceId)
      .referenceType(ReferenceTypeEnum.fromValue(resource.getMetadata.getResourceType.getValue))
      .workspaceId(resource.getMetadata.getWorkspaceId)
      .reference(new DataRepoSnapshot().instanceName(resource.getAttributes.getInstanceName).snapshot(resource.getAttributes.getSnapshot))
      .cloningInstructions(resource.getMetadata.getCloningInstructions)
  }

  private def snapshotListResponseToDataReferenceList(snapshotResponse: SnapshotListResponse): DataReferenceList= {
    import scala.collection.JavaConverters._
    val snaps = snapshotResponse.gcpDataRepoSnapshots.map{ res =>
      new DataReferenceDescription()
        .name(res.getMetadata.getName)
        .description(res.getMetadata.getDescription)
        .referenceId(res.getMetadata.getResourceId)
        .referenceType(ReferenceTypeEnum.fromValue(res.getMetadata.getResourceType.getValue))
        .workspaceId(res.getMetadata.getWorkspaceId)
        .reference(new DataRepoSnapshot().instanceName(res.getAttributes.getInstanceName).snapshot(res.getAttributes.getSnapshot))
        .cloningInstructions(res.getMetadata.getCloningInstructions)
    }.asJava
    new DataReferenceList().resources(snaps)
  }

}
