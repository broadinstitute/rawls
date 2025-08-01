package org.broadinstitute.dsde.rawls.webservice

/**
 * Created by tsharpe on 9/25/15.
 */

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import io.opentelemetry.context.Context
import org.broadinstitute.dsde.rawls.billing.BillingAdminService
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.submissions.SubmissionsService
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.workspace.WorkspaceAdminService
import spray.json.DefaultJsonProtocol._
import spray.json.JsString

import java.util.UUID
import scala.concurrent.ExecutionContext

trait AdminApiService extends UserInfoDirectives {
  implicit val executionContext: ExecutionContext

  import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
  import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._

  val workspaceAdminServiceConstructor: RawlsRequestContext => WorkspaceAdminService
  val submissionsServiceConstructor: RawlsRequestContext => SubmissionsService
  val userServiceConstructor: RawlsRequestContext => UserService
  val billingAdminServiceConstructor: RawlsRequestContext => BillingAdminService

  def adminRoutes(otelContext: Context = Context.root(), userInfo: UserInfo): server.Route = {
    val ctx = RawlsRequestContext(userInfo, Option(otelContext))
    path("admin" / "billing" / Segment) { projectId =>
      val billingProjectName = RawlsBillingProjectName(projectId)
      get {
        complete {
          billingAdminServiceConstructor(ctx)
            .getBillingProjectSupportSummary(billingProjectName)
            .map(StatusCodes.OK -> _)
        }
      } ~
        delete {
          entity(as[Map[String, String]]) { ownerInfo =>
            complete {
              userServiceConstructor(ctx)
                .adminDeleteBillingProject(billingProjectName, ownerInfo)
                .map(_ => StatusCodes.NoContent)
            }
          }
        }
    } ~
      path("admin" / "submissions") {
        get {
          complete {
            submissionsServiceConstructor(ctx).adminListAllActiveSubmissions()
          }
        }
      } ~
      path("admin" / "submissions" / Segment / Segment / Segment) { (workspaceNamespace, workspaceName, submissionId) =>
        delete {
          complete {
            submissionsServiceConstructor(ctx)
              .adminAbortSubmission(WorkspaceName(workspaceNamespace, workspaceName), submissionId)
              .map { count =>
                if (count == 1) StatusCodes.NoContent -> None
                else
                  StatusCodes.NotFound -> Option(
                    ErrorReport(StatusCodes.NotFound,
                                s"Unable to abort submission. Submission $submissionId could not be found."
                    )
                  )
              }
          }
        }
      } ~
      path("admin" / "submissions" / "queueStatusByUser") {
        get {
          complete {
            submissionsServiceConstructor(ctx).adminWorkflowQueueStatusByUser()
          }
        }
      } ~
      pathPrefix("admin" / "workspaces") {
        pathPrefix(Segment / Segment) { (workspaceNamespace, workspaceName) =>
          path("flags") {
            get {
              complete {
                workspaceAdminServiceConstructor(ctx).adminListWorkspaceFeatureFlags(
                  WorkspaceName(workspaceNamespace, workspaceName)
                )
              }
            } ~
              put {
                entity(as[List[String]]) { flagNames =>
                  complete {
                    workspaceAdminServiceConstructor(ctx).adminOverwriteWorkspaceFeatureFlags(
                      WorkspaceName(workspaceNamespace, workspaceName),
                      flagNames
                    )
                  }
                }
              }
          } ~
            path("deleteAzureWorkspace") {
              delete {
                complete {
                  workspaceAdminServiceConstructor(ctx)
                    .adminDeleteMcWorkspace(WorkspaceName(workspaceNamespace, workspaceName))
                    .map(_ => StatusCodes.NoContent)
                }
              }
            } ~
            path("id") {
              get {
                complete {
                  workspaceAdminServiceConstructor(ctx)
                    .getWorkspaceId(WorkspaceName(workspaceNamespace, workspaceName))
                    .map {
                      case Some(id) => StatusCodes.OK -> Option(JsString(id))
                      case None     => StatusCodes.NotFound -> None
                    }
                }
              }
            }
        } ~
          path(Segment) { workspaceId =>
            get {
              complete {
                workspaceAdminServiceConstructor(ctx)
                  .getWorkspaceById(UUID.fromString(workspaceId))
                  .map(StatusCodes.OK -> _)
              }
            }
          }
      }
  }
}
