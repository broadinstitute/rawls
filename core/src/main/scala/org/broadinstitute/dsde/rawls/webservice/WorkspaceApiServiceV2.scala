package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import io.opentelemetry.context.Context
import org.broadinstitute.dsde.rawls.bucketMigration.BucketMigrationService
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.monitor.migration.MultiregionalBucketMigrationJsonSupport._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.webservice.CustomDirectives.addLocationHeader
import org.broadinstitute.dsde.rawls.workspace.{WorkspaceService, WorkspaceSettingService}
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.ExecutionContext

trait WorkspaceApiServiceV2 extends UserInfoDirectives {
  implicit val executionContext: ExecutionContext

  val workspaceServiceConstructor: RawlsRequestContext => WorkspaceService
  val bucketMigrationServiceConstructor: RawlsRequestContext => BucketMigrationService
  val workspaceSettingServiceConstructor: RawlsRequestContext => WorkspaceSettingService

  def workspaceRoutesV2(otelContext: Context = Context.root(), userInfo: UserInfo): server.Route = {
    val ctx = RawlsRequestContext(userInfo, Option(otelContext))
    pathPrefix("workspaces" / "v2") {
      pathPrefix(Segment / Segment) { (namespace, name) =>
        val workspaceName = WorkspaceName(namespace, name)

        pathPrefix("clone") {
          post {
            entity(as[WorkspaceRequest]) { destWorkspace =>
              addLocationHeader(destWorkspace.toWorkspaceName.path) {
                complete {
                  workspaceServiceConstructor(ctx)
                    .cloneWorkspace(workspaceName, destWorkspace, ctx)
                    .map(workspace =>
                      StatusCodes.Created ->
                        WorkspaceDetails.fromWorkspaceAndOptions(
                          workspace,
                          Some(destWorkspace.authorizationDomain.getOrElse(Set.empty)),
                          useAttributes = true,
                          Some(WorkspaceCloudPlatform.Gcp)
                        )
                    )
                }
              }
            }
          }
        } ~
          pathPrefix("bucketUsage") {
            get {
              complete {
                workspaceServiceConstructor(ctx).getBucketUsageV2(workspaceName)
              }
            }
          } ~
          pathEndOrSingleSlash {
            delete {
              complete {
                workspaceServiceConstructor(ctx)
                  .deleteWorkspace(workspaceName)
                  .map(deletionResult => StatusCodes.Accepted -> deletionResult)
              }
            }
          } ~
          pathPrefix("bucketMigration") {
            pathEndOrSingleSlash {
              get {
                complete {
                  bucketMigrationServiceConstructor(ctx)
                    .getBucketMigrationAttemptsForWorkspace(workspaceName)
                    .map(ms => StatusCodes.OK -> ms)
                }
              } ~
                post {
                  complete {
                    bucketMigrationServiceConstructor(ctx)
                      .migrateWorkspaceBucket(workspaceName)
                      .map(StatusCodes.Created -> _)
                  }
                }
            } ~
              path("progress") {
                get {
                  complete {
                    bucketMigrationServiceConstructor(ctx)
                      .getBucketMigrationProgressForWorkspace(workspaceName)
                      .map(StatusCodes.OK -> _)
                  }
                }
              }
          } ~
          pathPrefix("settings") {
            pathEndOrSingleSlash {
              get {
                complete {
                  workspaceSettingServiceConstructor(ctx)
                    .getWorkspaceSettings(workspaceName)
                    .map(StatusCodes.OK -> _)
                }
              } ~
                put {
                  entity(as[List[WorkspaceSetting]]) { settings =>
                    complete {
                      workspaceSettingServiceConstructor(ctx)
                        .setWorkspaceSettings(workspaceName, settings)
                        .map(StatusCodes.OK -> _)
                    }
                  }
                }
            }
          } ~
          pathPrefix("authDomain") {
            pathEndOrSingleSlash {
              patch {
                entity(as[List[String]]) { newAuthDomainGroups =>
                  complete {
                    workspaceServiceConstructor(ctx)
                      .addAuthDomainGroups(workspaceName, newAuthDomainGroups.toSet)
                      .map(_ => StatusCodes.NoContent)
                  }
                }
              }
            }
          } ~
          pathPrefix("billingProject") {
            pathEndOrSingleSlash {
              patch {
                entity(as[WorkspaceRequestUpdateBilling]) { updateRequest =>
                  complete {
                    workspaceServiceConstructor(ctx)
                      .updateWorkspaceBillingProject(workspaceName, updateRequest.newBillingProjectName)
                      .map(_ => StatusCodes.OK)
                  }
                }
              }
            }
          } ~
          pathPrefix("repair") {
            pathEndOrSingleSlash {
              post {
                complete {
                  workspaceServiceConstructor(ctx)
                    .repairWorkspace(workspaceName)
                    .map(_ => StatusCodes.OK)
                }
              }
            }
          }
      } ~
        pathPrefix("bucketMigration") {
          pathEndOrSingleSlash {
            post {
              entity(as[List[WorkspaceName]]) { workspaceNames =>
                complete {
                  bucketMigrationServiceConstructor(ctx)
                    .migrateAllWorkspaceBuckets(workspaceNames)
                    .map(StatusCodes.Created -> _)
                }
              }
            } ~
              get {
                complete {
                  bucketMigrationServiceConstructor(ctx).getEligibleOrMigratingWorkspaces
                    .map(StatusCodes.OK -> _)
                }
              }
          } ~
            pathPrefix("getProgress") {
              pathEndOrSingleSlash {
                post {
                  entity(as[List[WorkspaceName]]) { workspaceNames =>
                    complete {
                      bucketMigrationServiceConstructor(ctx)
                        .getBucketMigrationProgressForWorkspaces(workspaceNames)
                        .map(StatusCodes.OK -> _)
                    }
                  }
                }
              }
            }
        }
    }
  }
}
