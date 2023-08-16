package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives.pathPrefix
import io.opencensus.scala.akka.http.TracingDirective.traceRequest
import org.broadinstitute.dsde.rawls.model.{RawlsRequestContext, WorkspaceName}
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.workspace.{MultiCloudWorkspaceService, WorkspaceService}

import scala.concurrent.ExecutionContext

trait WorkspaceApiServiceV2 extends UserInfoDirectives {
  implicit val executionContext: ExecutionContext

  val workspaceServiceConstructor: RawlsRequestContext => WorkspaceService
  val multiCloudWorkspaceServiceConstructor: RawlsRequestContext => MultiCloudWorkspaceService

  val workspaceRoutesV2: server.Route = traceRequest { span =>
    requireUserInfo(Option(span)) { userInfo =>
      val ctx = RawlsRequestContext(userInfo, Option(span))
      pathPrefix("workspaces" / "v2") {
        path(Segment / Segment) { (workspaceNamespace, workspaceName) =>
          delete {
            complete {
              val workspaceService = workspaceServiceConstructor(ctx)
              val mcWorkspaceService = multiCloudWorkspaceServiceConstructor(ctx)
              mcWorkspaceService
                .deleteMultiCloudOrRawlsWorkspaceV2(WorkspaceName(workspaceNamespace, workspaceName), workspaceService)
                .map(maybeBucketName => StatusCodes.Accepted -> "TODO")
            }
          }
        }
      }
    }
  }
}
