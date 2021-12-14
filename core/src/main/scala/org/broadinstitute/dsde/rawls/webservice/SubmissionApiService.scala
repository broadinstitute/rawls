package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport.ErrorReportFormat
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import spray.json.DefaultJsonProtocol._
import spray.json.{JsString, PrettyPrinter}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

/**
  * Created by dvoet on 6/4/15.
  */
trait SubmissionApiService extends UserInfoDirectives {
  implicit val executionContext: ExecutionContext

  val workspaceServiceConstructor: UserInfo => WorkspaceService
  val submissionTimeout: FiniteDuration


  val submissionRoutes: server.Route = requireUserInfo() { userInfo =>
    path("workspaces" / Segment / Segment / "submissions") { (workspaceNamespace, workspaceName) =>
      get {
        complete { workspaceServiceConstructor(userInfo).listSubmissions(WorkspaceName(workspaceNamespace, workspaceName)) }
      }
    } ~
      path("workspaces" / Segment / Segment / "submissionsCount") { (workspaceNamespace, workspaceName) =>
        get {
          complete { workspaceServiceConstructor(userInfo).countSubmissions(WorkspaceName(workspaceNamespace, workspaceName)) }
        }
      } ~
      path("workspaces" / Segment / Segment / "submissions") { (workspaceNamespace, workspaceName) =>
        post {
          entity(as[SubmissionRequest]) { submission =>
            complete { workspaceServiceConstructor(userInfo).createSubmission(WorkspaceName(workspaceNamespace, workspaceName), submission).map(StatusCodes.Created -> _) }
          }
        }
      } ~
      path("workspaces" / Segment / Segment / "submissions" / "validate") { (workspaceNamespace, workspaceName) =>
        post {
          entity(as[SubmissionRequest]) { submission =>
            complete { workspaceServiceConstructor(userInfo).validateSubmission(WorkspaceName(workspaceNamespace, workspaceName), submission) }
          }
        }
      } ~
      path("workspaces" / Segment / Segment / "submissions" / Segment) { (workspaceNamespace, workspaceName, submissionId) =>
        get {
          complete { workspaceServiceConstructor(userInfo).getSubmissionStatus(WorkspaceName(workspaceNamespace, workspaceName), submissionId) }
        }
      } ~
      path("workspaces" / Segment / Segment / "submissions" / Segment) { (workspaceNamespace, workspaceName, submissionId) =>
        patch {
          entity(as[UserCommentUpdateOperation]) { newComment =>
            complete {
              workspaceServiceConstructor(userInfo).updateSubmissionUserComment(WorkspaceName(workspaceNamespace, workspaceName), submissionId, newComment).map { rowsUpdated =>
                if (rowsUpdated == 1) StatusCodes.NoContent -> None
                else StatusCodes.NotFound -> Option(ErrorReport(StatusCodes.NotFound, s"Unable to update userComment for submission. Submission ${submissionId} could not be found."))
              }
            }
          }
        }
      } ~
      path("workspaces" / Segment / Segment / "submissions" / Segment) { (workspaceNamespace, workspaceName, submissionId) =>
        delete {
          complete {
            workspaceServiceConstructor(userInfo).abortSubmission(WorkspaceName(workspaceNamespace, workspaceName), submissionId).map { count =>
              if(count == 1) StatusCodes.NoContent -> None
              else StatusCodes.NotFound -> Option(s"Unable to abort submission. Submission ${submissionId} could not be found.")
            }
          }
        }
      } ~
      path("workspaces" / Segment / Segment / "submissions" / Segment / "workflows" / Segment) { (workspaceNamespace, workspaceName, submissionId, workflowId) =>
        get {
          parameters("includeKey".as[String].*, "excludeKey".as[String].*, "expandSubWorkflows".as[Boolean] ? false) { (includes, excludes, expandSubWorkflows) =>
            complete { workspaceServiceConstructor(userInfo).workflowMetadata(WorkspaceName(workspaceNamespace, workspaceName),
              submissionId, workflowId, MetadataParams(includes.toSet, excludes.toSet, expandSubWorkflows)) }
          }
        }
      } ~
      path("workspaces" / Segment / Segment / "submissions" / Segment / "workflows" / Segment / "outputs") { (workspaceNamespace, workspaceName, submissionId, workflowId) =>
        get {
          complete { workspaceServiceConstructor(userInfo).workflowOutputs(WorkspaceName(workspaceNamespace, workspaceName), submissionId, workflowId) }
        }
      } ~
      path("workspaces" / Segment / Segment / "submissions" / Segment / "workflows" / Segment / "cost") { (workspaceNamespace, workspaceName, submissionId, workflowId) =>
        get {
          complete { workspaceServiceConstructor(userInfo).workflowCost(WorkspaceName(workspaceNamespace, workspaceName), submissionId, workflowId) }
        }
      } ~
      path("workflows" / Segment / "genomics" / Segments) { (workflowId, operationId) =>
        get {
          complete {
            workspaceServiceConstructor(userInfo).getGenomicsOperationV2(workflowId, operationId).map {
              case Some(jsobj) =>
                implicit val printer = PrettyPrinter
                StatusCodes.OK -> jsobj
              case None => StatusCodes.NotFound -> JsString(s"jobId ${operationId.mkString("/")} not found.")
            }
          }
        }
      } ~
      path("submissions" / "queueStatus") {
        get {
          complete { workspaceServiceConstructor(userInfo).workflowQueueStatus }
        }
      } ~
      path("submissions" / "userWorkflowCapacity") {
        get {
          complete { workspaceServiceConstructor(userInfo).userWorkflowCapacity }
        }
      }
  }
}
