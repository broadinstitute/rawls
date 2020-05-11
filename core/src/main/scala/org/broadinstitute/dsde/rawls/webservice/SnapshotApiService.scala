package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import org.broadinstitute.dsde.rawls.model.{DataRepoSnapshot, UserInfo, WorkspaceName, EnumerateSnapshotRequestBody}
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.snapshot.SnapshotService
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import org.broadinstitute.dsde.rawls.model.DataReferenceModelJsonSupport._

import scala.concurrent.ExecutionContext

trait SnapshotApiService extends UserInfoDirectives {

  implicit val executionContext: ExecutionContext

  val snapshotServiceConstructor: UserInfo => SnapshotService

  val snapshotRoutes: server.Route = requireUserInfo() { userInfo =>
    path("workspaces" / Segment / Segment / "snapshots") { (workspaceNamespace, workspaceName) =>
      post {
        entity(as[DataRepoSnapshot]) { dataRepoSnapshot =>
          complete {
            snapshotServiceConstructor(userInfo).CreateSnapshot(WorkspaceName(workspaceNamespace, workspaceName), dataRepoSnapshot).map(StatusCodes.Created -> _)
          }
        }
      }
      get {
        entity(as[EnumerateSnapshotRequestBody]) { body =>
          complete {
            snapshotServiceConstructor(userInfo).ListSnapshots(WorkspaceName(workspaceNamespace, workspaceName), body.offset, body.limit).map(StatusCodes.Created -> _)
          }
        }
      }
    } ~
    path("workspaces" / Segment / Segment / "snapshots" / Segment) { (workspaceNamespace, workspaceName, snapshotId) =>
      get {
        complete {
          snapshotServiceConstructor(userInfo).GetSnapshot(WorkspaceName(workspaceNamespace, workspaceName), snapshotId).map(StatusCodes.OK -> _)
        }
      }
    }
  }
}
