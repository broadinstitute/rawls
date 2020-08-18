package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import org.broadinstitute.dsde.rawls.model.DataReferenceModelJsonSupport._
import org.broadinstitute.dsde.rawls.model.{NamedDataRepoSnapshot, UserInfo, WorkspaceName}
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.snapshot.SnapshotService

import scala.concurrent.ExecutionContext

trait SnapshotApiService extends UserInfoDirectives {

  implicit val executionContext: ExecutionContext

  val snapshotServiceConstructor: UserInfo => SnapshotService

  val snapshotRoutes: server.Route = requireUserInfo() { userInfo =>
    path("workspaces" / Segment / Segment / "snapshots") { (workspaceNamespace, workspaceName) =>
      post {
        entity(as[NamedDataRepoSnapshot]) { namedDataRepoSnapshot =>
          complete {
            snapshotServiceConstructor(userInfo).CreateSnapshot(WorkspaceName(workspaceNamespace, workspaceName), namedDataRepoSnapshot).map(StatusCodes.Created -> _)
          }
        }
      } ~
      get {
        parameters("offset".as[Int], "limit".as[Int]) { (offset, limit) =>
          complete {
            snapshotServiceConstructor(userInfo).EnumerateSnapshots(WorkspaceName(workspaceNamespace, workspaceName), offset, limit).map(StatusCodes.OK -> _)
          }
        }
      }
    } ~
    path("workspaces" / Segment / Segment / "snapshots" / Segment) { (workspaceNamespace, workspaceName, snapshotId) =>
      get {
        complete {
          snapshotServiceConstructor(userInfo).GetSnapshot(WorkspaceName(workspaceNamespace, workspaceName), snapshotId).map(StatusCodes.OK -> _)
        }
      } ~
      delete {
        complete {
          snapshotServiceConstructor(userInfo).DeleteSnapshot(WorkspaceName(workspaceNamespace, workspaceName), snapshotId).map(_ => StatusCodes.NoContent)
        }
      }
    }
  }
}
