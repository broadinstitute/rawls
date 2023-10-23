package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import io.opencensus.scala.akka.http.TracingDirective._
import org.broadinstitute.dsde.rawls.bucketMigration.BucketMigrationService
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.monitor.migration.MultiregionalBucketMigrationJsonSupport._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.workspace.{MultiCloudWorkspaceService, WorkspaceService}
import spray.json.DefaultJsonProtocol._
import spray.json.{JsObject, _}

import scala.concurrent.ExecutionContext

trait WorkspaceApiServiceV2 extends UserInfoDirectives {
  implicit val executionContext: ExecutionContext

  val workspaceServiceConstructor: RawlsRequestContext => WorkspaceService
  val multiCloudWorkspaceServiceConstructor: RawlsRequestContext => MultiCloudWorkspaceService
  val bucketMigrationServiceConstructor: RawlsRequestContext => BucketMigrationService

  val workspaceRoutesV2: server.Route = traceRequest { span =>
    requireUserInfo(Option(span)) { userInfo =>
      val ctx = RawlsRequestContext(userInfo, Option(span))
      pathPrefix("workspaces" / "v2") {
        pathPrefix(Segment / Segment) { (namespace, name) =>
          val workspaceName = WorkspaceName(namespace, name)
          pathEndOrSingleSlash {
            delete {
              complete {
                val workspaceService = workspaceServiceConstructor(ctx)
                val mcWorkspaceService = multiCloudWorkspaceServiceConstructor(ctx)
                mcWorkspaceService
                  .deleteMultiCloudOrRawlsWorkspaceV2(workspaceName, workspaceService)
                  .map(result => StatusCodes.Accepted -> JsObject(Map("result" -> result.toJson)))

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
}
