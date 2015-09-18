package org.broadinstitute.dsde.rawls.webservice

import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import spray.httpx.SprayJsonSupport._
import spray.routing._

/**
 * Created by dvoet on 6/4/15.
 */
trait SubmissionApiService extends HttpService with PerRequestCreator with UserInfoDirectives {
  lazy private implicit val executionContext = actorRefFactory.dispatcher

  val workspaceServiceConstructor: UserInfo => WorkspaceService

  val submissionRoutes = requireUserInfo() { userInfo =>
    path("workspaces" / Segment / Segment / "submissions" ) { (workspaceNamespace, workspaceName) =>
      get {
        requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
          WorkspaceService.ListSubmissions(WorkspaceName(workspaceNamespace, workspaceName)))
      }
    } ~
    path("workspaces" / Segment / Segment / "submissions") { (workspaceNamespace, workspaceName) =>
      post {
        entity(as[SubmissionRequest]) { submission =>
          requestContext => perRequest(requestContext,
            WorkspaceService.props(workspaceServiceConstructor, userInfo),
            WorkspaceService.CreateSubmission(WorkspaceName(workspaceNamespace, workspaceName), submission))
        }
      }
    } ~
    path("workspaces" / Segment / Segment / "submissions" / "validate") { (workspaceNamespace, workspaceName) =>
      post {
        entity(as[SubmissionRequest]) { submission =>
          requestContext => perRequest(requestContext,
            WorkspaceService.props(workspaceServiceConstructor, userInfo),
            WorkspaceService.ValidateSubmission(WorkspaceName(workspaceNamespace, workspaceName), submission))
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
    } ~
      path("workspaces" / Segment / Segment / "submissions" / Segment / "workflows" / Segment ) { (workspaceNamespace, workspaceName, submissionId, workflowId) =>
        get {
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
            WorkspaceService.GetWorkflowMetadata(WorkspaceName(workspaceNamespace, workspaceName), submissionId, workflowId))
        }
      } ~
      path("workspaces" / Segment / Segment / "submissions" / Segment / "workflows" / Segment / "outputs") { (workspaceNamespace, workspaceName, submissionId, workflowId) =>
        get {
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
            WorkspaceService.GetWorkflowOutputs(WorkspaceName(workspaceNamespace, workspaceName), submissionId, workflowId))
        }
    }
  }
}