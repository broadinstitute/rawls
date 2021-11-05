package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import io.opencensus.scala.akka.http.TracingDirective._
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.AttributeUpdateOperation
import org.broadinstitute.dsde.rawls.model.WorkspaceACLJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.webservice.CustomDirectives._
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import spray.json.DefaultJsonProtocol._

import scala.concurrent.ExecutionContext

/**
  * Created by dvoet on 6/4/15.
  */

trait WorkspaceApiService extends UserInfoDirectives {
  import PerRequest.requestCompleteMarshaller
  implicit val executionContext: ExecutionContext

  val workspaceServiceConstructor: UserInfo => WorkspaceService

  val workspaceRoutes: server.Route = requireUserInfo() { userInfo =>
    path("workspaces") {
      post {
        entity(as[WorkspaceRequest]) { workspace =>
          addLocationHeader(workspace.path) {
            traceRequest { span =>
              complete {
                workspaceServiceConstructor(userInfo).createWorkspace(workspace, span).map(w => StatusCodes.Created -> WorkspaceDetails(w, workspace.authorizationDomain.getOrElse(Set.empty)))
              }
            }
          }
        }
      } ~
        get {
          parameterSeq { allParams =>
            traceRequest { span =>
              complete {
                workspaceServiceConstructor(userInfo).listWorkspaces(WorkspaceFieldSpecs.fromQueryParams(allParams, "fields"), span)
              }
            }
          }
        }
    } ~
      path("workspaces" / Segment / Segment) { (workspaceNamespace, workspaceName) =>
        patch {
          entity(as[Array[AttributeUpdateOperation]]) { operations =>
            complete { workspaceServiceConstructor(userInfo).updateWorkspace(WorkspaceName(workspaceNamespace, workspaceName), operations) }
          }
        } ~
          get {
            parameterSeq { allParams =>
              traceRequest { span =>
                complete {
                  workspaceServiceConstructor(userInfo).getWorkspace(WorkspaceName(workspaceNamespace, workspaceName),
                    WorkspaceFieldSpecs.fromQueryParams(allParams, "fields"), span)
                }
              }
            }
          } ~
          delete {
            traceRequest { span =>
              complete { workspaceServiceConstructor(userInfo).deleteWorkspace(WorkspaceName(workspaceNamespace, workspaceName), span) }
            }
          }
      } ~
      path("workspaces" / Segment / Segment / "accessInstructions") { (workspaceNamespace, workspaceName) =>
        get {
          complete { workspaceServiceConstructor(userInfo).getAccessInstructions(WorkspaceName(workspaceNamespace, workspaceName)) }
        }
      } ~
      path("workspaces" / Segment / Segment / "bucketOptions") { (workspaceNamespace, workspaceName) =>
        get {
          complete { workspaceServiceConstructor(userInfo).getBucketOptions(WorkspaceName(workspaceNamespace, workspaceName)) }
        }
      } ~
      path("workspaces" / Segment / Segment / "clone") { (sourceNamespace, sourceWorkspace) =>
        post {
          entity(as[WorkspaceRequest]) { destWorkspace =>
            addLocationHeader(destWorkspace.toWorkspaceName.path) {
              traceRequest { span =>
                complete {
                  workspaceServiceConstructor(userInfo).cloneWorkspace(WorkspaceName(sourceNamespace, sourceWorkspace), destWorkspace, span).map(w => StatusCodes.Created -> WorkspaceDetails(w, destWorkspace.authorizationDomain.getOrElse(Set.empty)))
                }
              }
            }
          }
        }
      } ~
      path("workspaces" / Segment / Segment / "acl") { (workspaceNamespace, workspaceName) =>
        get {
          complete { workspaceServiceConstructor(userInfo).getACL(WorkspaceName(workspaceNamespace, workspaceName)) }
        } ~
          patch {
            parameter('inviteUsersNotFound.?) { inviteUsersNotFound =>
              entity(as[Set[WorkspaceACLUpdate]]) { aclUpdate =>
                complete { workspaceServiceConstructor(userInfo).updateACL(WorkspaceName(workspaceNamespace, workspaceName), aclUpdate, inviteUsersNotFound.getOrElse("false").toBoolean) }
              }
            }
          }
      } ~
      path("workspaces" / Segment / Segment / "library") { (workspaceNamespace, workspaceName) =>
        patch {
          entity(as[Array[AttributeUpdateOperation]]) { operations =>
            complete { workspaceServiceConstructor(userInfo).updateLibraryAttributes(WorkspaceName(workspaceNamespace, workspaceName), operations) }
          }
        }
      } ~
      path("workspaces" / Segment / Segment / "catalog") { (workspaceNamespace, workspaceName) =>
        get {
          complete { workspaceServiceConstructor(userInfo).getCatalog(WorkspaceName(workspaceNamespace, workspaceName)) }
        } ~
          patch {
            entity(as[Array[WorkspaceCatalog]]) { catalogUpdate =>
              complete { workspaceServiceConstructor(userInfo).updateCatalog(WorkspaceName(workspaceNamespace, workspaceName), catalogUpdate) }
            }
          }
      } ~
      path("workspaces" / Segment / Segment / "checkBucketReadAccess") { (workspaceNamespace, workspaceName) =>
        get {
          complete { workspaceServiceConstructor(userInfo).checkBucketReadAccess(WorkspaceName(workspaceNamespace, workspaceName)) }
        }
      } ~
      path("workspaces" / Segment / Segment / "checkIamActionWithLock" / Segment) { (workspaceNamespace, workspaceName, requiredAction) =>
        get {
          complete { workspaceServiceConstructor(userInfo).checkSamActionWithLock(WorkspaceName(workspaceNamespace, workspaceName), SamResourceAction(requiredAction)) }
        }
      } ~
      path("workspaces" / Segment / Segment / "lock") { (workspaceNamespace, workspaceName) =>
        put {
          complete { workspaceServiceConstructor(userInfo).lockWorkspace(WorkspaceName(workspaceNamespace, workspaceName)) }
        }
      } ~
      path("workspaces" / Segment / Segment / "unlock") { (workspaceNamespace, workspaceName) =>
        put {
          complete { workspaceServiceConstructor(userInfo).unlockWorkspace(WorkspaceName(workspaceNamespace, workspaceName)) }
        }
      } ~
      path("workspaces" / Segment / Segment / "bucketUsage") { (workspaceNamespace, workspaceName) =>
        get {
          complete { workspaceServiceConstructor(userInfo).getBucketUsage(WorkspaceName(workspaceNamespace, workspaceName)) }
        }
      } ~
      path("workspaces" / "tags") {
        parameter('q.?) { queryString =>
          get {
            complete { workspaceServiceConstructor(userInfo).getTags(queryString) }
          }
        }
      } ~
      path("workspaces" / Segment / Segment / "sendChangeNotification") { (namespace, name) =>
        post {
          complete { workspaceServiceConstructor(userInfo).sendChangeNotifications(WorkspaceName(namespace, name)) }
        }
      } ~
      path("workspaces" / Segment / Segment / "enableRequesterPaysForLinkedServiceAccounts") { (workspaceNamespace, workspaceName) =>
        put {
          complete { workspaceServiceConstructor(userInfo).enableRequesterPaysForLinkedSAs(WorkspaceName(workspaceNamespace, workspaceName)) }
        }
      } ~
      path("workspaces" / Segment / Segment / "disableRequesterPaysForLinkedServiceAccounts") { (workspaceNamespace, workspaceName) =>
        put {
          complete { workspaceServiceConstructor(userInfo).disableRequesterPaysForLinkedSAs(WorkspaceName(workspaceNamespace, workspaceName)) }
        }
      }
  }
}
