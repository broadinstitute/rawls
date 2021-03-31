package org.broadinstitute.dsde.rawls.webservice

import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.marshalling.ToResponseMarshallable
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
        complete {
          workspaceServiceConstructor(userInfo).listSubmissions(WorkspaceName(workspaceNamespace, workspaceName)).map(StatusCodes.OK -> _)
        }
      }
    } ~
      path("workspaces" / Segment / Segment / "submissionsCount") { (workspaceNamespace, workspaceName) =>
        get {
          complete {
            workspaceServiceConstructor(userInfo).countSubmissions(WorkspaceName(workspaceNamespace, workspaceName)).map(StatusCodes.OK -> _)
          }
        }
      } ~
      path("workspaces" / Segment / Segment / "submissions") { (workspaceNamespace, workspaceName) =>
        post {
          entity(as[SubmissionRequest]) { submission =>
            complete {
              workspaceServiceConstructor(userInfo).createSubmission(WorkspaceName(workspaceNamespace, workspaceName), submission).map(StatusCodes.Created -> _)
            }
          }
        }
      } ~
      path("workspaces" / Segment / Segment / "submissions" / "validate") { (workspaceNamespace, workspaceName) =>
        post {
          entity(as[SubmissionRequest]) { submission =>
            complete {
              workspaceServiceConstructor(userInfo).validateSubmission(WorkspaceName(workspaceNamespace, workspaceName), submission).map(StatusCodes.OK -> _)
            }
          }
        }
      } ~
      path("workspaces" / Segment / Segment / "submissions" / Segment) { (workspaceNamespace, workspaceName, submissionId) =>
        get {
          complete {
            workspaceServiceConstructor(userInfo).getSubmissionStatus(WorkspaceName(workspaceNamespace, workspaceName), submissionId).map(StatusCodes.OK -> _)
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
            complete {
              workspaceServiceConstructor(userInfo).workflowMetadata(WorkspaceName(workspaceNamespace, workspaceName),
              submissionId, workflowId, MetadataParams(includes.toSet, excludes.toSet, expandSubWorkflows)).map(StatusCodes.OK -> _)
            }
          }
        }
      } ~
      path("workspaces" / Segment / Segment / "submissions" / Segment / "workflows" / Segment / "outputs") { (workspaceNamespace, workspaceName, submissionId, workflowId) =>
        get {
          complete {
            workspaceServiceConstructor(userInfo).workflowOutputs(WorkspaceName(workspaceNamespace, workspaceName), submissionId, workflowId).map(StatusCodes.OK -> _)
          }
        }
      } ~
      path("workspaces" / Segment / Segment / "submissions" / Segment / "workflows" / Segment / "cost") { (workspaceNamespace, workspaceName, submissionId, workflowId) =>
        get {
          complete {
            workspaceServiceConstructor(userInfo).workflowCost(WorkspaceName(workspaceNamespace, workspaceName), submissionId, workflowId).map(StatusCodes.OK -> _)
          }
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
          complete {
            workspaceServiceConstructor(userInfo).workflowQueueStatus.map(StatusCodes.OK -> _)
          }
        }
      }
  }
}