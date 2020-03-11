package org.broadinstitute.dsde.rawls.webservice

import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.AttributeUpdateOperation
import org.broadinstitute.dsde.rawls.model.WorkspaceACLJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import spray.json.DefaultJsonProtocol._
import akka.http.scaladsl.model.StatusCodes
import CustomDirectives._

import scala.concurrent.ExecutionContext
import io.opencensus.scala.akka.http.TracingDirective._

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
                workspaceServiceConstructor(userInfo).CreateWorkspace(workspace, span).map(w => StatusCodes.Created -> WorkspaceDetails(w, workspace.authorizationDomain.getOrElse(Set.empty)))
              }
            }
          }
        }
      } ~
        get {
          parameterSeq { allParams =>
            traceRequest { span =>
              complete {
                workspaceServiceConstructor(userInfo).ListWorkspaces(WorkspaceFieldSpecs.fromQueryParams(allParams, "fields"), span)
              }
            }
          }
        }
    } ~
      path("workspaces" / Segment / Segment) { (workspaceNamespace, workspaceName) =>
        patch {
          entity(as[Array[AttributeUpdateOperation]]) { operations =>
            complete { workspaceServiceConstructor(userInfo).UpdateWorkspace(WorkspaceName(workspaceNamespace, workspaceName), operations) }
          }
        } ~
          get {
            parameterSeq { allParams =>
              traceRequest { span =>
                complete {
                  workspaceServiceConstructor(userInfo).GetWorkspace(WorkspaceName(workspaceNamespace, workspaceName),
                    WorkspaceFieldSpecs.fromQueryParams(allParams, "fields"), span)
                }
              }
            }
          } ~
          delete {
            complete { workspaceServiceConstructor(userInfo).DeleteWorkspace(WorkspaceName(workspaceNamespace, workspaceName)) }
          }
      } ~
      path("workspaces" / Segment / Segment / "accessInstructions") { (workspaceNamespace, workspaceName) =>
        get {
          complete { workspaceServiceConstructor(userInfo).GetAccessInstructions(WorkspaceName(workspaceNamespace, workspaceName)) }
        }
      } ~
      path("workspaces" / Segment / Segment / "bucketOptions") { (workspaceNamespace, workspaceName) =>
        get {
          complete { workspaceServiceConstructor(userInfo).GetBucketOptions(WorkspaceName(workspaceNamespace, workspaceName)) }
        }
      } ~
      path("workspaces" / Segment / Segment / "clone") { (sourceNamespace, sourceWorkspace) =>
        post {
          entity(as[WorkspaceRequest]) { destWorkspace =>
            addLocationHeader(destWorkspace.toWorkspaceName.path) {
              complete {
                workspaceServiceConstructor(userInfo).CloneWorkspace(WorkspaceName(sourceNamespace, sourceWorkspace), destWorkspace).map(w => StatusCodes.Created -> WorkspaceDetails(w, destWorkspace.authorizationDomain.getOrElse(Set.empty)))
              }
            }
          }
        }
      } ~
      path("workspaces" / Segment / Segment / "acl") { (workspaceNamespace, workspaceName) =>
        get {
          complete { workspaceServiceConstructor(userInfo).GetACL(WorkspaceName(workspaceNamespace, workspaceName)) }
        } ~
          patch {
            parameter('inviteUsersNotFound.?) { inviteUsersNotFound =>
              entity(as[Set[WorkspaceACLUpdate]]) { aclUpdate =>
                complete { workspaceServiceConstructor(userInfo).UpdateACL(WorkspaceName(workspaceNamespace, workspaceName), aclUpdate, inviteUsersNotFound.getOrElse("false").toBoolean) }
              }
            }
          }
      } ~
      path("workspaces" / Segment / Segment / "library") { (workspaceNamespace, workspaceName) =>
        patch {
          entity(as[Array[AttributeUpdateOperation]]) { operations =>
            complete { workspaceServiceConstructor(userInfo).UpdateLibraryAttributes(WorkspaceName(workspaceNamespace, workspaceName), operations) }
          }
        }
      } ~
      path("workspaces" / Segment / Segment / "catalog") { (workspaceNamespace, workspaceName) =>
        get {
          complete { workspaceServiceConstructor(userInfo).GetCatalog(WorkspaceName(workspaceNamespace, workspaceName)) }
        } ~
          patch {
            entity(as[Array[WorkspaceCatalog]]) { catalogUpdate =>
              complete { workspaceServiceConstructor(userInfo).UpdateCatalog(WorkspaceName(workspaceNamespace, workspaceName), catalogUpdate) }
            }
          }
      } ~
      path("workspaces" / Segment / Segment / "checkBucketReadAccess") { (workspaceNamespace, workspaceName) =>
        get {
          complete { workspaceServiceConstructor(userInfo).CheckBucketReadAccess(WorkspaceName(workspaceNamespace, workspaceName)) }
        }
      } ~
      path("workspaces" / Segment / Segment / "checkIamActionWithLock" / Segment) { (workspaceNamespace, workspaceName, requiredAction) =>
        get {
          complete { workspaceServiceConstructor(userInfo).CheckSamActionWithLock(WorkspaceName(workspaceNamespace, workspaceName), SamResourceAction(requiredAction)) }
        }
      } ~
      path("workspaces" / Segment / Segment / "lock") { (workspaceNamespace, workspaceName) =>
        put {
          complete { workspaceServiceConstructor(userInfo).LockWorkspace(WorkspaceName(workspaceNamespace, workspaceName)) }
        }
      } ~
      path("workspaces" / Segment / Segment / "unlock") { (workspaceNamespace, workspaceName) =>
        put {
          complete { workspaceServiceConstructor(userInfo).UnlockWorkspace(WorkspaceName(workspaceNamespace, workspaceName)) }
        }
      } ~
      path("workspaces" / Segment / Segment / "bucketUsage") { (workspaceNamespace, workspaceName) =>
        get {
          complete { workspaceServiceConstructor(userInfo).GetBucketUsage(WorkspaceName(workspaceNamespace, workspaceName)) }
        }
      } ~
      path("workspaces" / "tags") {
        parameter('q.?) { queryString =>
          get {
            complete { workspaceServiceConstructor(userInfo).GetTags(queryString) }
          }
        }
      } ~
      path("workspaces" / Segment / Segment / "sendChangeNotification") { (namespace, name) =>
        post {
          complete { workspaceServiceConstructor(userInfo).SendChangeNotifications(WorkspaceName(namespace, name)) }
        }
      } ~
      path("workspaces" / Segment / Segment / "enableRequesterPaysForLinkedServiceAccounts") { (workspaceNamespace, workspaceName) =>
        put {
          complete { workspaceServiceConstructor(userInfo).EnableRequesterPaysForLinkedSAs(WorkspaceName(workspaceNamespace, workspaceName)) }
        }
      } ~
      path("workspaces" / Segment / Segment / "disableRequesterPaysForLinkedServiceAccounts") { (workspaceNamespace, workspaceName) =>
        put {
          complete { workspaceServiceConstructor(userInfo).DisableRequesterPaysForLinkedSAs(WorkspaceName(workspaceNamespace, workspaceName)) }
        }
      }
  }
}
