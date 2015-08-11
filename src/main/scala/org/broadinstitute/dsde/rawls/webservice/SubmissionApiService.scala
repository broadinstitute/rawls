package org.broadinstitute.dsde.rawls.webservice

import java.util.logging.Logger
import javax.ws.rs.Path
import com.wordnik.swagger.annotations._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport._
import org.broadinstitute.dsde.rawls.openam.OpenAmDirectives
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import spray.httpx.SprayJsonSupport._
import spray.routing._

/**
 * Created by dvoet on 6/4/15.
 */
trait SubmissionApiService extends HttpService with PerRequestCreator with OpenAmDirectives {
  lazy private implicit val executionContext = actorRefFactory.dispatcher

  val workspaceServiceConstructor: UserInfo => WorkspaceService

  val submissionRoutes = userInfoFromCookie() { userInfo =>
    path("workspaces" / Segment / Segment / "submissions") { (workspaceNamespace, workspaceName) =>
      post {
        entity(as[SubmissionRequest]) { submission =>
          requestContext => perRequest(requestContext,
            WorkspaceService.props(workspaceServiceConstructor, userInfo),
            WorkspaceService.CreateSubmission(WorkspaceName(workspaceNamespace, workspaceName), submission))
        }
      }
    } ~
    path("workspaces" / Segment / Segment / "submissions" / Segment) { (workspaceNamespace, workspaceName, submissionId) =>
      get {
        requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
          WorkspaceService.GetSubmissionStatus(WorkspaceName(workspaceNamespace, workspaceName), submissionId))
      }
    } ~
    path("workspaces" / Segment / Segment / "submissions" / Segment) { (workspaceNamespace, workspaceName, submissionId) =>
      delete {
        requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
          WorkspaceService.AbortSubmission(WorkspaceName(workspaceNamespace, workspaceName), submissionId))
      }
    }
  }
}