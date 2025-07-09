package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import io.opentelemetry.context.Context
import org.broadinstitute.dsde.rawls.model.{RawlsRequestContext, UserInfo, WorkspaceName}
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.snapshot.SnapshotService
import spray.json.DefaultJsonProtocol._

import java.util.UUID
import scala.concurrent.ExecutionContext

trait SnapshotApiService extends UserInfoDirectives {

  implicit val executionContext: ExecutionContext

  val snapshotServiceConstructor: RawlsRequestContext => SnapshotService

  def snapshotRoutes(otelContext: Context = Context.root(), userInfo: UserInfo): server.Route = {
    val ctx = RawlsRequestContext(userInfo, Option(otelContext))
    path("workspaces" / Segment / Segment / "snapshots" / "v3") { (workspaceNamespace, workspaceName) =>
      post {
        entity(as[Set[String]]) { snapshotIds =>
          complete {
            snapshotServiceConstructor(ctx)
              .createSnapshotsByWorkspaceNameV3(WorkspaceName(workspaceNamespace, workspaceName),
                                                snapshotIds.map(UUID.fromString)
              )
              .map(_ => StatusCodes.NoContent)
          }
        }
      }
    } ~
      path("workspaces" / Segment / "snapshots" / "v3") { workspaceId =>
        post {
          entity(as[Set[String]]) { snapshotIds =>
            complete {
              snapshotServiceConstructor(ctx)
                .createSnapshotsByWorkspaceIdV3(workspaceId, snapshotIds.map(UUID.fromString))
                .map(_ => StatusCodes.NoContent)
            }
          }
        }
      } ~
      path("workspaces" / Segment / Segment / "snapshots" / "v2") { (workspaceNamespace, workspaceName) =>
        get {
          // for backwards compatibility, we return a hardcoded empty list instead of throwing an error.
          // this allows callers of this API to not see errors.
          complete(Seq.empty[String])
        }
      } ~
      path("workspaces" / Segment / "snapshots" / "v2") { workspaceId =>
        get {
          // for backwards compatibility, we return a hardcoded empty list instead of throwing an error.
          // this allows callers of this API to not see errors.
          complete(Seq.empty[String])
        }
      }
  }
}
