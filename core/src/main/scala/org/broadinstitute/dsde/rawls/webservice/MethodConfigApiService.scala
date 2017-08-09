package org.broadinstitute.dsde.rawls.webservice

import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import spray.routing.Directive.pimpApply
import spray.routing._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import spray.httpx.SprayJsonSupport._

import scala.concurrent.ExecutionContext

/**
 * Created by dvoet on 6/4/15.
 */

trait MethodConfigApiService extends HttpService with PerRequestCreator with UserInfoDirectives {
  implicit val executionContext: ExecutionContext

  val workspaceServiceConstructor: UserInfo => WorkspaceService

  val methodConfigRoutes = requireUserInfo() { userInfo =>
    path("workspaces" / Segment / Segment / "methodconfigs") { (workspaceNamespace, workspaceName) =>
      get {
        requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
          WorkspaceService.ListMethodConfigurations(WorkspaceName(workspaceNamespace, workspaceName)))
      } ~
      post {
        entity(as[MethodConfiguration]) { methodConfiguration =>
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
            WorkspaceService.CreateMethodConfiguration(WorkspaceName(workspaceNamespace, workspaceName), methodConfiguration))
        }
      }
    } ~
    path("workspaces" / Segment / Segment / "methodconfigs" / Segment / Segment) { (workspaceNamespace, workspaceName, methodConfigurationNamespace, methodConfigName) =>
      get {
        requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
          WorkspaceService.GetMethodConfiguration(WorkspaceName(workspaceNamespace, workspaceName), methodConfigurationNamespace, methodConfigName))
      } ~
      put {
        entity(as[MethodConfiguration]) { newMethodConfiguration =>
          requestContext => {
            perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
              WorkspaceService.UpdateMethodConfiguration(WorkspaceName(workspaceNamespace, workspaceName), methodConfigurationNamespace, methodConfigName, newMethodConfiguration))
          }
        }
      } ~
      delete {
        requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
          WorkspaceService.DeleteMethodConfiguration(WorkspaceName(workspaceNamespace, workspaceName), methodConfigurationNamespace, methodConfigName))
      }
    } ~
    path("workspaces" / Segment / Segment / "methodconfigs" / Segment / Segment / "validate") { (workspaceNamespace, workspaceName, methodConfigurationNamespace, methodConfigName) =>
      get {
        requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
          WorkspaceService.GetAndValidateMethodConfiguration(WorkspaceName(workspaceNamespace, workspaceName), methodConfigurationNamespace, methodConfigName))
      }
    } ~
    path("workspaces" / Segment / Segment / "methodconfigs" / Segment / Segment / "rename") { (workspaceNamespace, workspaceName, methodConfigurationNamespace, methodConfigurationName) =>
      post {
        entity(as[MethodConfigurationName]) { newEntityName =>
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
            WorkspaceService.RenameMethodConfiguration(WorkspaceName(workspaceNamespace, workspaceName), methodConfigurationNamespace, methodConfigurationName, newEntityName.name))
        }
      }
    } ~
    path("methodconfigs" / "copy") {
      post {
        entity(as[MethodConfigurationNamePair]) { confNames =>
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
            WorkspaceService.CopyMethodConfiguration(confNames))
        }
      }
    } ~
    path("methodconfigs" / "copyFromMethodRepo") {
      post {
        entity(as[MethodRepoConfigurationImport]) { query =>
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
            WorkspaceService.CopyMethodConfigurationFromMethodRepo(query))
        }
      }
    } ~
    path("methodconfigs" / "copyToMethodRepo") {
      post {
        entity(as[MethodRepoConfigurationExport]) { query =>
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
            WorkspaceService.CopyMethodConfigurationToMethodRepo(query))
        }
      }
    } ~
    path("methodconfigs" / "template") {
      post {
        entity(as[MethodRepoMethod]) { methodRepoMethod =>
          requestContext => perRequest(requestContext,
                                        WorkspaceService.props(workspaceServiceConstructor, userInfo),
                                        WorkspaceService.CreateMethodConfigurationTemplate(methodRepoMethod))
        }
      }
    } ~
    path("methodconfigs" / "inputsOutputs") {
      post {
        entity(as[MethodRepoMethod]) { methodRepoMethod =>
          requestContext => perRequest(requestContext,
            WorkspaceService.props(workspaceServiceConstructor, userInfo),
            WorkspaceService.GetMethodInputsOutputs(methodRepoMethod))
        }
      }
    }
  }
}