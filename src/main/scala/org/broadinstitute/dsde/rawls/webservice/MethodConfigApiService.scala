package org.broadinstitute.dsde.rawls.webservice

import kamon.spray.KamonTraceDirectives._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import spray.routing.Directive.pimpApply
import spray.routing._

import scala.concurrent.ExecutionContext

/**
 * Created by dvoet on 6/4/15.
 */

trait MethodConfigApiService extends HttpService with PerRequestCreator with UserInfoDirectives {
  implicit val executionContext: ExecutionContext

  import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
  import spray.httpx.SprayJsonSupport._

  val workspaceServiceConstructor: UserInfo => WorkspaceService

  val methodConfigRoutes = requireUserInfo() { userInfo =>
    path("workspaces" / Segment / Segment / "methodconfigs") { (workspaceNamespace, workspaceName) =>
      post {
        traceName("CreateMethodConfiguration") {
          entity(as[MethodConfiguration]) { methodConfiguration =>
            requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
              WorkspaceService.CreateMethodConfiguration(WorkspaceName(workspaceNamespace, workspaceName), methodConfiguration))
          }
        }
      }
    } ~
    path("workspaces" / Segment / Segment / "methodconfigs" / Segment / Segment) { (workspaceNamespace, workspaceName, methodConfigurationNamespace, methodConfigName) =>
      get {
        traceName("GetMethodConfiguration") {
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
            WorkspaceService.GetMethodConfiguration(WorkspaceName(workspaceNamespace, workspaceName), methodConfigurationNamespace, methodConfigName))
        }
      }
    } ~
    path("workspaces" / Segment / Segment / "methodconfigs" / Segment / Segment / "validate") { (workspaceNamespace, workspaceName, methodConfigurationNamespace, methodConfigName) =>
      get {
        traceName("GetAndValidateMethodConfiguration") {
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
            WorkspaceService.GetAndValidateMethodConfiguration(WorkspaceName(workspaceNamespace, workspaceName), methodConfigurationNamespace, methodConfigName))
        }
      }
    } ~
    path("workspaces" / Segment / Segment / "methodconfigs") { (workspaceNamespace, workspaceName) =>
      get {
        traceName("ListMethodConfigurations") {
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
            WorkspaceService.ListMethodConfigurations(WorkspaceName(workspaceNamespace, workspaceName)))
        }
      }
    } ~
    path("workspaces" / Segment / Segment / "methodconfigs" / Segment / Segment) { (workspaceNamespace, workspaceName, methodConfigurationNamespace, methodConfigName) =>
      delete {
        traceName("DeleteMethodConfiguration") {
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
            WorkspaceService.DeleteMethodConfiguration(WorkspaceName(workspaceNamespace, workspaceName), methodConfigurationNamespace, methodConfigName))
        }
      }
    } ~
    path("workspaces" / Segment / Segment / "methodconfigs" / Segment / Segment / "rename") { (workspaceNamespace, workspaceName, methodConfigurationNamespace, methodConfigurationName) =>
      post {
        traceName("RenameMethodConfiguration") {
          entity(as[MethodConfigurationName]) { newEntityName =>
            requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
              WorkspaceService.RenameMethodConfiguration(WorkspaceName(workspaceNamespace, workspaceName), methodConfigurationNamespace, methodConfigurationName, newEntityName.name))
          }
        }
      }
    } ~
    path("workspaces" / Segment / Segment / "methodconfigs" / Segment / Segment) { (workspaceNamespace, workspaceName, methodConfigurationNamespace, methodConfigName) =>
      put {
        traceName("UpdateMethodConfiguration") {
          entity(as[MethodConfiguration]) { newMethodConfiguration =>
            requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
              WorkspaceService.UpdateMethodConfiguration(WorkspaceName(workspaceNamespace, workspaceName), newMethodConfiguration.copy(namespace = methodConfigurationNamespace, name = methodConfigName)))
          }
        }
      }
    } ~
    path("methodconfigs" / "copy") {
      post {
        traceName("CopyMethodConfiguration") {
          entity(as[MethodConfigurationNamePair]) { confNames =>
            requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
              WorkspaceService.CopyMethodConfiguration(confNames))
          }
        }
      }
    } ~
    path("methodconfigs" / "copyFromMethodRepo") {
      post {
        traceName("CopyMethodConfigurationFromMethodRepo") {
          entity(as[MethodRepoConfigurationImport]) { query =>
            requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
              WorkspaceService.CopyMethodConfigurationFromMethodRepo(query))
          }
        }
      }
    } ~
    path("methodconfigs" / "copyToMethodRepo") {
      post {
        traceName("CopyMethodConfigurationToMethodRepo") {
          entity(as[MethodRepoConfigurationExport]) { query =>
            requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
              WorkspaceService.CopyMethodConfigurationToMethodRepo(query))
          }
        }
      }
    } ~
    path("methodconfigs" / "template") {
      post {
        traceName("CreateMethodConfigurationTemplate") {
          entity(as[MethodRepoMethod]) { methodRepoMethod =>
            requestContext => perRequest(requestContext,
              WorkspaceService.props(workspaceServiceConstructor, userInfo),
              WorkspaceService.CreateMethodConfigurationTemplate(methodRepoMethod))
          }
        }
      }
    }
  }
}