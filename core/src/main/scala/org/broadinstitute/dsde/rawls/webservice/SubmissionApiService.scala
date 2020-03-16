package org.broadinstitute.dsde.rawls.webservice

import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

/**
  * Created by dvoet on 6/4/15.
  */
trait SubmissionApiService extends UserInfoDirectives {
  import PerRequest.requestCompleteMarshaller
  implicit val executionContext: ExecutionContext

  val workspaceServiceConstructor: UserInfo => WorkspaceService
  val submissionTimeout: FiniteDuration


  val submissionRoutes: server.Route = requireUserInfo() { userInfo =>
    path("workspaces" / Segment / Segment / "submissions") { (workspaceNamespace, workspaceName) =>
      get {
        complete { workspaceServiceConstructor(userInfo).ListSubmissions(WorkspaceName(workspaceNamespace, workspaceName)) }
      }
    } ~
      path("workspaces" / Segment / Segment / "submissionsCount") { (workspaceNamespace, workspaceName) =>
        get {
          complete { workspaceServiceConstructor(userInfo).CountSubmissions(WorkspaceName(workspaceNamespace, workspaceName)) }
        }
      } ~
      path("workspaces" / Segment / Segment / "submissions") { (workspaceNamespace, workspaceName) =>
        post {
          entity(as[SubmissionRequest]) { submission =>
            complete { workspaceServiceConstructor(userInfo).CreateSubmission(WorkspaceName(workspaceNamespace, workspaceName), submission) },
          }
        }
      } ~
      path("workspaces" / Segment / Segment / "submissions" / "validate") { (workspaceNamespace, workspaceName) =>
        post {
          entity(as[SubmissionRequest]) { submission =>
            complete { workspaceServiceConstructor(userInfo).ValidateSubmission(WorkspaceName(workspaceNamespace, workspaceName), submission) }
          }
        }
      } ~
      path("workspaces" / Segment / Segment / "submissions" / Segment) { (workspaceNamespace, workspaceName, submissionId) =>
        get {
          complete { workspaceServiceConstructor(userInfo).GetSubmissionStatus(WorkspaceName(workspaceNamespace, workspaceName), submissionId) }
        }
      } ~
      path("workspaces" / Segment / Segment / "submissions" / Segment) { (workspaceNamespace, workspaceName, submissionId) =>
        delete {
          complete { workspaceServiceConstructor(userInfo).AbortSubmission(WorkspaceName(workspaceNamespace, workspaceName), submissionId) }
        }
      } ~
      path("workspaces" / Segment / Segment / "submissions" / Segment / "workflows" / Segment) { (workspaceNamespace, workspaceName, submissionId, workflowId) =>
        get {
          parameters("includeKey".as[String].*, "excludeKey".as[String].*, "expandSubWorkflows".as[Boolean] ? false) { (includes, excludes, expandSubWorkflows) =>
            complete { workspaceServiceConstructor(userInfo).GetWorkflowMetadata(WorkspaceName(workspaceNamespace, workspaceName),
              submissionId, workflowId, MetadataParams(includes.toSet, excludes.toSet, expandSubWorkflows)) }
          }
        }
      } ~
      path("workspaces" / Segment / Segment / "submissions" / Segment / "workflows" / Segment / "outputs") { (workspaceNamespace, workspaceName, submissionId, workflowId) =>
        get {
          complete { workspaceServiceConstructor(userInfo).GetWorkflowOutputs(WorkspaceName(workspaceNamespace, workspaceName), submissionId, workflowId) }
        }
      } ~
      path("workspaces" / Segment / Segment / "submissions" / Segment / "workflows" / Segment / "cost") { (workspaceNamespace, workspaceName, submissionId, workflowId) =>
        get {
          complete { workspaceServiceConstructor(userInfo).GetWorkflowCost(WorkspaceName(workspaceNamespace, workspaceName), submissionId, workflowId) }
        }
      } ~
      path("workflows" / Segment / "genomics" / Segments) { (workflowId, operationId) =>
        get {
          complete { workspaceServiceConstructor(userInfo).GetGenomicsOperationV2(workflowId, operationId) }
        }
      } ~
      path("submissions" / "queueStatus") {
        get {
          complete { workspaceServiceConstructor(userInfo).WorkflowQueueStatus }
        }
      }
  }
}