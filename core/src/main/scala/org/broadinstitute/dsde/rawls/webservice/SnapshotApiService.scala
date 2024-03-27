package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import io.opentelemetry.context.Context
import bio.terra.workspace.model._
import org.broadinstitute.dsde.rawls.model.DataReferenceModelJsonSupport._
import org.broadinstitute.dsde.rawls.model.{
  NamedDataRepoSnapshot,
  RawlsRequestContext,
  UserInfo,
  Workspace,
  WorkspaceFieldSpecs,
  WorkspaceName,
  WorkspaceResponse
}
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.snapshot.SnapshotService
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService

import java.util.UUID
import scala.concurrent.ExecutionContext

trait SnapshotApiService extends UserInfoDirectives {

  implicit val executionContext: ExecutionContext

  val snapshotServiceConstructor: RawlsRequestContext => SnapshotService

  def snapshotRoutes(otelContext: Context = Context.root()): server.Route =
    requireUserInfo(Option(otelContext)) { userInfo =>
      val ctx = RawlsRequestContext(userInfo, Option(otelContext))
      path("workspaces" / Segment / Segment / "snapshots" / "v2") { (workspaceNamespace, workspaceName) =>
        post {
          entity(as[NamedDataRepoSnapshot]) { namedDataRepoSnapshot =>
            complete {
              snapshotServiceConstructor(ctx)
                .createSnapshotByWorkspaceName(WorkspaceName(workspaceNamespace, workspaceName), namedDataRepoSnapshot)
                .map(StatusCodes.Created -> _)
            }
          }
        } ~
          get {
            // N.B. the "as[UUID]" delegates to SnapshotService.validateSnapshotId, which is in scope;
            // that method provides a 400 Bad Request response and nice error message
            parameters("offset".as[Int], "limit".as[Int], "referencedSnapshotId".as[UUID].optional) {
              (offset, limit, referencedSnapshotId) =>
                complete {
                  snapshotServiceConstructor(ctx).enumerateSnapshotsByWorkspaceName(WorkspaceName(workspaceNamespace,
                                                                                                  workspaceName
                                                                                    ),
                                                                                    offset,
                                                                                    limit,
                                                                                    referencedSnapshotId
                  )
                }
            }
          }
      } ~
        path("workspaces" / Segment / Segment / "snapshots" / "v2" / Segment) {
          (workspaceNamespace, workspaceName, snapshotId) =>
            get {
              complete {
                snapshotServiceConstructor(ctx).getSnapshotResourceFromWsm(WorkspaceName(workspaceNamespace,
                                                                                         workspaceName
                                                                           ),
                                                                           snapshotId
                )
              }
            } ~
              patch {
                entity(as[UpdateDataRepoSnapshotReferenceRequestBody]) { updateDataRepoSnapshotReferenceRequestBody =>
                  complete {
                    snapshotServiceConstructor(ctx)
                      .updateSnapshot(WorkspaceName(workspaceNamespace, workspaceName),
                                      snapshotId,
                                      updateDataRepoSnapshotReferenceRequestBody
                      )
                      .map(_ => StatusCodes.NoContent)
                  }
                }
              } ~
              delete {
                complete {
                  snapshotServiceConstructor(ctx)
                    .deleteSnapshot(WorkspaceName(workspaceNamespace, workspaceName), snapshotId)
                    .map(_ => StatusCodes.NoContent)
                }
              }
        } ~
        path("workspaces" / Segment / Segment / "snapshots" / "v2" / "name" / Segment) {
          (workspaceNamespace, workspaceName, referenceName) =>
            get {
              complete {
                snapshotServiceConstructor(ctx).getSnapshotByName(WorkspaceName(workspaceNamespace, workspaceName),
                                                                  referenceName
                )
              }
            }
        } ~
        path("workspaces" / Segment / "snapshots" / "v2") { workspaceId =>
          post {
            entity(as[NamedDataRepoSnapshot]) { namedDataRepoSnapshot =>
              complete {
                snapshotServiceConstructor(ctx)
                  .createSnapshotByWorkspaceId(workspaceId, namedDataRepoSnapshot)
                  .map(StatusCodes.Created -> _)
              }
            }
          } ~
            get {
              // N.B. the "as[UUID]" delegates to SnapshotService.validateSnapshotId, which is in scope;
              // that method provides a 400 Bad Request response and nice error message
              parameters("offset".as[Int], "limit".as[Int], "referencedSnapshotId".as[UUID].optional) {
                (offset, limit, referencedSnapshotId) =>
                  complete {
                    snapshotServiceConstructor(ctx).enumerateSnapshotsById(workspaceId,
                                                                           offset,
                                                                           limit,
                                                                           referencedSnapshotId
                    )
                  }
              }
            }
        }
    }
}
