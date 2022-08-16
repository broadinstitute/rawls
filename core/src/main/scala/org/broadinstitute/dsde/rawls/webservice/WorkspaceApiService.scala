package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import io.opencensus.scala.akka.http.TracingDirective._
import io.opencensus.trace.Span
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.AttributeUpdateOperation
import org.broadinstitute.dsde.rawls.model.WorkspaceACLJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.webservice.CustomDirectives._
import org.broadinstitute.dsde.rawls.workspace.{MultiCloudWorkspaceService, WorkspaceService}
import spray.json.DefaultJsonProtocol._

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

/** Created by dvoet on 6/4/15.
  */

trait WorkspaceApiService extends UserInfoDirectives {
  implicit val executionContext: ExecutionContext

  val workspaceServiceConstructor: UserInfo => WorkspaceService
  val multiCloudWorkspaceServiceConstructor: UserInfo => MultiCloudWorkspaceService

  val workspaceRoutes: server.Route = requireUserInfo() { userInfo =>
    path("workspaces") {
      post {
        entity(as[WorkspaceRequest]) { workspace =>
          addLocationHeader(workspace.path) {
            traceRequest { span =>
              complete {
                val workspaceService = workspaceServiceConstructor(userInfo)
                val mcWorkspaceService =
                  multiCloudWorkspaceServiceConstructor(userInfo)
                mcWorkspaceService
                  .createMultiCloudOrRawlsWorkspace(
                    workspace,
                    workspaceService,
                    span
                  )
                  .map(w =>
                    StatusCodes.Created -> WorkspaceDetails(
                      w,
                      workspace.authorizationDomain.getOrElse(Set.empty)
                    )
                  )
              }
            }
          }
        }
      } ~
        get {
          parameterSeq { allParams =>
            traceRequest { span =>
              complete {
                workspaceServiceConstructor(userInfo).listWorkspaces(
                  WorkspaceFieldSpecs.fromQueryParams(allParams, "fields"),
                  span
                )
              }
            }
          }
        }
    } ~
      path("workspaces" / "tags") {
        parameters('q.?, "limit".as[Int].optional) { (queryString, limit) =>
          get {
            complete {
              workspaceServiceConstructor(userInfo).getTags(queryString, limit)
            }
          }
        }
      } ~
      path("workspaces" / "id" / Segment) { workspaceId =>
        get {
          parameterSeq { allParams =>
            traceRequest { span =>
              complete {
                workspaceServiceConstructor(userInfo).getWorkspaceById(
                  workspaceId,
                  WorkspaceFieldSpecs.fromQueryParams(allParams, "fields"),
                  span
                )
              }
            }
          }
        }
      } ~
      path("workspaces" / Segment / Segment) { (workspaceNamespace, workspaceName) =>
        /* we enforce a 6-character minimum for workspaceNamespace, as part of billing project creation.
           the previous "mc", "tags", and "id" paths rely on this convention to avoid path-matching conflicts.
           we might want to change the first Segment above to a regex a la """[^/.]{6,}""".r
           but note that would be a behavior change: if a user entered fewer than 6 chars it would result in an
           unmatched path rejection instead of the custom error handling inside WorkspaceService.
         */

        patch {
          entity(as[Array[AttributeUpdateOperation]]) { operations =>
            complete {
              workspaceServiceConstructor(userInfo).updateWorkspace(
                WorkspaceName(workspaceNamespace, workspaceName),
                operations
              )
            }
          }
        } ~
          get {
            parameterSeq { allParams =>
              traceRequest { span =>
                complete {
                  workspaceServiceConstructor(userInfo).getWorkspace(
                    WorkspaceName(workspaceNamespace, workspaceName),
                    WorkspaceFieldSpecs.fromQueryParams(allParams, "fields"),
                    span
                  )
                }
              }
            }
          } ~
          delete {
            traceRequest { span =>
              complete {
                workspaceServiceConstructor(userInfo)
                  .deleteWorkspace(
                    WorkspaceName(workspaceNamespace, workspaceName),
                    span
                  )
                  .map(maybeBucketName =>
                    StatusCodes.Accepted -> workspaceDeleteMessage(
                      maybeBucketName
                    )
                  )
              }
            }
          }
      } ~
      path("workspaces" / Segment / Segment / "accessInstructions") { (workspaceNamespace, workspaceName) =>
        get {
          complete {
            workspaceServiceConstructor(userInfo).getAccessInstructions(
              WorkspaceName(workspaceNamespace, workspaceName)
            )
          }
        }
      } ~
      path("workspaces" / Segment / Segment / "bucketOptions") { (workspaceNamespace, workspaceName) =>
        get {
          complete {
            workspaceServiceConstructor(userInfo).getBucketOptions(
              WorkspaceName(workspaceNamespace, workspaceName)
            )
          }
        }
      } ~
      path("workspaces" / Segment / Segment / "clone") { (sourceNamespace, sourceWorkspace) =>
        post {
          entity(as[WorkspaceRequest]) { destWorkspace =>
            addLocationHeader(destWorkspace.toWorkspaceName.path) {
              traceRequest { span =>
                complete {
                  workspaceServiceConstructor(userInfo)
                    .cloneWorkspace(
                      WorkspaceName(sourceNamespace, sourceWorkspace),
                      destWorkspace,
                      span
                    )
                    .map(w =>
                      StatusCodes.Created -> WorkspaceDetails(
                        w,
                        destWorkspace.authorizationDomain.getOrElse(Set.empty)
                      )
                    )
                }
              }
            }
          }
        }
      } ~
      path("workspaces" / Segment / Segment / "acl") { (workspaceNamespace, workspaceName) =>
        get {
          complete {
            workspaceServiceConstructor(userInfo).getACL(
              WorkspaceName(workspaceNamespace, workspaceName)
            )
          }
        } ~
          patch {
            parameter('inviteUsersNotFound.?) { inviteUsersNotFound =>
              entity(as[Set[WorkspaceACLUpdate]]) { aclUpdate =>
                complete {
                  workspaceServiceConstructor(userInfo).updateACL(
                    WorkspaceName(workspaceNamespace, workspaceName),
                    aclUpdate,
                    inviteUsersNotFound.getOrElse("false").toBoolean
                  )
                }
              }
            }
          }
      } ~
      path("workspaces" / Segment / Segment / "library") { (workspaceNamespace, workspaceName) =>
        patch {
          entity(as[Array[AttributeUpdateOperation]]) { operations =>
            complete {
              workspaceServiceConstructor(userInfo).updateLibraryAttributes(
                WorkspaceName(workspaceNamespace, workspaceName),
                operations
              )
            }
          }
        }
      } ~
      path("workspaces" / Segment / Segment / "catalog") { (workspaceNamespace, workspaceName) =>
        get {
          complete {
            workspaceServiceConstructor(userInfo).getCatalog(
              WorkspaceName(workspaceNamespace, workspaceName)
            )
          }
        } ~
          patch {
            entity(as[Array[WorkspaceCatalog]]) { catalogUpdate =>
              complete {
                workspaceServiceConstructor(userInfo).updateCatalog(
                  WorkspaceName(workspaceNamespace, workspaceName),
                  catalogUpdate
                )
              }
            }
          }
      } ~
      path("workspaces" / Segment / Segment / "checkBucketReadAccess") { (workspaceNamespace, workspaceName) =>
        get {
          complete {
            workspaceServiceConstructor(userInfo)
              .checkBucketReadAccess(
                WorkspaceName(workspaceNamespace, workspaceName)
              )
              .map(_ => StatusCodes.OK)
          }
        }
      } ~
      path(
        "workspaces" / Segment / Segment / "checkIamActionWithLock" / Segment
      ) { (workspaceNamespace, workspaceName, requiredAction) =>
        get {
          complete {
            workspaceServiceConstructor(userInfo)
              .checkSamActionWithLock(
                WorkspaceName(workspaceNamespace, workspaceName),
                SamResourceAction(requiredAction)
              )
              .map {
                case true  => StatusCodes.NoContent
                case false => StatusCodes.Forbidden
              }
          }
        }
      } ~
      path("workspaces" / Segment / Segment / "fileTransfers") { (workspaceNamespace, workspaceName) =>
        get {
          complete {
            workspaceServiceConstructor(userInfo)
              .listPendingFileTransfersForWorkspace(
                WorkspaceName(workspaceNamespace, workspaceName)
              )
              .map(pendingTransfers => StatusCodes.OK -> pendingTransfers)
          }
        }
      } ~
      path("workspaces" / Segment / Segment / "lock") { (workspaceNamespace, workspaceName) =>
        put {
          complete {
            workspaceServiceConstructor(userInfo)
              .lockWorkspace(WorkspaceName(workspaceNamespace, workspaceName))
              .map(_ => StatusCodes.NoContent)
          }
        }
      } ~
      path("workspaces" / Segment / Segment / "unlock") { (workspaceNamespace, workspaceName) =>
        put {
          complete {
            workspaceServiceConstructor(userInfo)
              .unlockWorkspace(
                WorkspaceName(workspaceNamespace, workspaceName)
              )
              .map(_ => StatusCodes.NoContent)
          }
        }
      } ~
      path("workspaces" / Segment / Segment / "bucketUsage") { (workspaceNamespace, workspaceName) =>
        get {
          complete {
            workspaceServiceConstructor(userInfo).getBucketUsage(
              WorkspaceName(workspaceNamespace, workspaceName)
            )
          }
        }
      } ~
      path("workspaces" / Segment / Segment / "sendChangeNotification") { (namespace, name) =>
        post {
          complete {
            workspaceServiceConstructor(userInfo).sendChangeNotifications(
              WorkspaceName(namespace, name)
            )
          }
        }
      } ~
      path(
        "workspaces" / Segment / Segment / "enableRequesterPaysForLinkedServiceAccounts"
      ) { (workspaceNamespace, workspaceName) =>
        put {
          complete {
            workspaceServiceConstructor(userInfo)
              .enableRequesterPaysForLinkedSAs(
                WorkspaceName(workspaceNamespace, workspaceName)
              )
              .map(_ => StatusCodes.NoContent)
          }
        }
      } ~
      path(
        "workspaces" / Segment / Segment / "disableRequesterPaysForLinkedServiceAccounts"
      ) { (workspaceNamespace, workspaceName) =>
        put {
          complete {
            workspaceServiceConstructor(userInfo)
              .disableRequesterPaysForLinkedSAs(
                WorkspaceName(workspaceNamespace, workspaceName)
              )
              .map(_ => StatusCodes.NoContent)
          }
        }
      }
  }

  private def workspaceDeleteMessage(
    maybeGoogleBucket: Option[String]
  ): String =
    maybeGoogleBucket match {
      case Some(bucketName) =>
        s"Your Google bucket $bucketName will be deleted within 24h."
      case None => "Your workspace has been deleted."
    }
}
