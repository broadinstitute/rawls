package org.broadinstitute.dsde.rawls.webservice

/**
 * Created by tsharpe on 9/25/15.
 */

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import io.opentelemetry.context.Context
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.bucketMigration.{BucketMigrationService, BucketMigrationServiceImpl}
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.monitor.migration.MultiregionalBucketMigrationJsonSupport._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.submissions.SubmissionsService
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.workspace.WorkspaceAdminService
import spray.json.DefaultJsonProtocol._

import java.util.UUID
import scala.concurrent.ExecutionContext

trait AdminApiService extends UserInfoDirectives {
  implicit val executionContext: ExecutionContext

  import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
  import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._

  // val workspaceServiceConstructor: RawlsRequestContext => WorkspaceService
  val workspaceAdminServiceConstructor: RawlsRequestContext => WorkspaceAdminService
  val submissionsServiceConstructor: RawlsRequestContext => SubmissionsService
  val userServiceConstructor: RawlsRequestContext => UserService
  val bucketMigrationServiceConstructor: RawlsRequestContext => BucketMigrationService

  def adminRoutes(otelContext: Context = Context.root()): server.Route = {
    requireUserInfo(Option(otelContext)) { userInfo =>
      val ctx = RawlsRequestContext(userInfo, Option(otelContext))
      path("admin" / "billing" / Segment) { projectId =>
        delete {
          entity(as[Map[String, String]]) { ownerInfo =>
            complete {
              userServiceConstructor(ctx)
                .adminDeleteBillingProject(RawlsBillingProjectName(projectId), ownerInfo)
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
        path("admin" / "submissions" / Segment / Segment / Segment) {
          (workspaceNamespace, workspaceName, submissionId) =>
            delete {
              complete {
                submissionsServiceConstructor(ctx)
                  .adminAbortSubmission(WorkspaceName(workspaceNamespace, workspaceName), submissionId)
                  .map { count =>
                    if (count == 1) StatusCodes.NoContent -> None
                    else
                      StatusCodes.NotFound -> Option(
                        ErrorReport(StatusCodes.NotFound,
                                    s"Unable to abort submission. Submission ${submissionId} could not be found."
                        )
                      )
                  }
              }
            }
        } ~
        path("admin" / "submissions" / "queueStatusByUser") {
          get {
            complete {
              submissionsServiceConstructor(ctx).adminWorkflowQueueStatusByUser
            }
          }
        } ~
        path("admin" / "user" / "role" / "curator" / Segment) { userEmail =>
          put {
            complete {
              userServiceConstructor(ctx).adminAddLibraryCurator(RawlsUserEmail(userEmail)).map(_ => StatusCodes.OK)
            }
          } ~
            delete {
              complete {
                userServiceConstructor(ctx)
                  .adminRemoveLibraryCurator(RawlsUserEmail(userEmail))
                  .map(_ => StatusCodes.OK)
              }
            }
        } ~
        path("admin" / "workspaces") {
          get {
            parameters('attributeName.?, 'valueString.?, 'valueNumber.?, 'valueBoolean.?) {
              (nameOption, stringOption, numberOption, booleanOption) =>
                val resultFuture = nameOption match {
                  case None => workspaceAdminServiceConstructor(ctx).listAllWorkspaces()
                  case Some(attributeName) =>
                    val name = AttributeName.fromDelimitedName(attributeName)
                    (stringOption, numberOption, booleanOption) match {
                      case (Some(string), None, None) =>
                        workspaceAdminServiceConstructor(ctx).adminListWorkspacesWithAttribute(name,
                                                                                               AttributeString(string)
                        )
                      case (None, Some(number), None) =>
                        workspaceAdminServiceConstructor(ctx).adminListWorkspacesWithAttribute(
                          name,
                          AttributeNumber(number.toDouble)
                        )
                      case (None, None, Some(boolean)) =>
                        workspaceAdminServiceConstructor(ctx).adminListWorkspacesWithAttribute(
                          name,
                          AttributeBoolean(boolean.toBoolean)
                        )
                      case _ =>
                        throw new RawlsException("Specify exactly one of valueString, valueNumber, or valueBoolean")
                    }
                }
                complete {
                  resultFuture
                }
            }
          }
        } ~
        pathPrefix("admin" / "bucketMigration") {
          pathPrefix("workspaces") {
            pathEndOrSingleSlash {
              post {
                entity(as[List[WorkspaceName]]) { workspaceNames =>
                  complete {
                    bucketMigrationServiceConstructor(ctx)
                      .adminMigrateAllWorkspaceBuckets(workspaceNames)
                      .map(StatusCodes.Created -> _)
                  }
                }
              }
            } ~
              pathPrefix("getProgress") {
                pathEndOrSingleSlash {
                  post {
                    entity(as[List[WorkspaceName]]) { workspaceNames =>
                      complete {
                        bucketMigrationServiceConstructor(ctx)
                          .adminGetBucketMigrationProgressForWorkspaces(workspaceNames)
                          .map(StatusCodes.OK -> _)
                      }
                    }
                  }
                }
              } ~
              pathPrefix(Segment / Segment) { (namespace, name) =>
                val workspaceName = WorkspaceName(namespace, name)
                pathEndOrSingleSlash {
                  get {
                    complete {
                      bucketMigrationServiceConstructor(ctx)
                        .adminGetBucketMigrationAttemptsForWorkspace(workspaceName)
                        .map(ms => StatusCodes.OK -> ms)
                    }
                  } ~
                    post {
                      complete {
                        bucketMigrationServiceConstructor(ctx)
                          .adminMigrateWorkspaceBucket(workspaceName)
                          .map(StatusCodes.Created -> _)
                      }
                    }
                } ~
                  path("progress") {
                    get {
                      complete {
                        bucketMigrationServiceConstructor(ctx)
                          .adminGetBucketMigrationProgressForWorkspace(workspaceName)
                          .map(StatusCodes.OK -> _)
                      }
                    }
                  }
              }
          } ~
            pathPrefix("billing" / Segment) { projectName =>
              val billingProjectName = RawlsBillingProjectName(projectName)
              pathEndOrSingleSlash {
                post {
                  complete {
                    bucketMigrationServiceConstructor(ctx)
                      .adminMigrateWorkspaceBucketsInBillingProject(billingProjectName)
                      .map(StatusCodes.Created -> _)
                  }
                } ~
                  get {
                    complete {
                      bucketMigrationServiceConstructor(ctx)
                        .adminGetBucketMigrationAttemptsForBillingProject(billingProjectName)
                        .map(ms => StatusCodes.OK -> ms)
                    }
                  }
              } ~
                path("progress") {
                  get {
                    complete {
                      bucketMigrationServiceConstructor(ctx)
                        .adminGetBucketMigrationProgressForBillingProject(billingProjectName)
                        .map(StatusCodes.OK -> _)
                    }
                  }
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
}
