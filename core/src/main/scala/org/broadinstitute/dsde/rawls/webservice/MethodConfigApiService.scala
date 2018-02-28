package org.broadinstitute.dsde.rawls.webservice

import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import WorkspaceJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import CustomDirectives._

import scala.concurrent.ExecutionContext

/**
  * Created by dvoet on 6/4/15.
  */

trait MethodConfigApiService extends UserInfoDirectives {
  import PerRequest.requestCompleteMarshaller
  implicit val executionContext: ExecutionContext

  val workspaceServiceConstructor: UserInfo => WorkspaceService

  val methodConfigRoutes: server.Route = requireUserInfo() { userInfo =>
    path("workspaces" / Segment / Segment / "methodconfigs") { (workspaceNamespace, workspaceName) =>
      get {
        parameters( "allRepos".as[Boolean] ? false ) { allRepos =>
          if (allRepos) {
            complete { workspaceServiceConstructor(userInfo).ListMethodConfigurations(WorkspaceName(workspaceNamespace, workspaceName)) }
          } else {
            complete { workspaceServiceConstructor(userInfo).ListAgoraMethodConfigurations(WorkspaceName(workspaceNamespace, workspaceName)) }
          }
        }
      } ~
        post {
          entity(as[MethodConfiguration]) { methodConfiguration =>
            addLocationHeader(methodConfiguration.path(WorkspaceName(workspaceNamespace, workspaceName))) {
              complete {
                workspaceServiceConstructor(userInfo).CreateMethodConfiguration(WorkspaceName(workspaceNamespace, workspaceName), methodConfiguration).map(StatusCodes.Created -> _)
              }
            }
          }
        }
    } ~
      path("workspaces" / Segment / Segment / "methodconfigs" / Segment / Segment) { (workspaceNamespace, workspaceName, methodConfigurationNamespace, methodConfigName) =>
        get {
          complete { workspaceServiceConstructor(userInfo).GetMethodConfiguration(WorkspaceName(workspaceNamespace, workspaceName), methodConfigurationNamespace, methodConfigName) }
        } ~
          put {
            entity(as[MethodConfiguration]) { newMethodConfiguration =>
              addLocationHeader(newMethodConfiguration.path(WorkspaceName(workspaceNamespace, workspaceName))) {
                complete {
                  workspaceServiceConstructor(userInfo).OverwriteMethodConfiguration(WorkspaceName(workspaceNamespace, workspaceName), methodConfigurationNamespace, methodConfigName, newMethodConfiguration).map(StatusCodes.OK -> _)
                }
              }
            }
          } ~
          post {
            entity(as[MethodConfiguration]) { newMethodConfiguration =>
              complete { workspaceServiceConstructor(userInfo).UpdateMethodConfiguration(WorkspaceName(workspaceNamespace, workspaceName), methodConfigurationNamespace, methodConfigName, newMethodConfiguration) }
            }
          } ~
          delete {
            complete { workspaceServiceConstructor(userInfo).DeleteMethodConfiguration(WorkspaceName(workspaceNamespace, workspaceName), methodConfigurationNamespace, methodConfigName) }
          }
      } ~
      path("workspaces" / Segment / Segment / "methodconfigs" / Segment / Segment / "validate") { (workspaceNamespace, workspaceName, methodConfigurationNamespace, methodConfigName) =>
        get {
          complete { workspaceServiceConstructor(userInfo).GetAndValidateMethodConfiguration(WorkspaceName(workspaceNamespace, workspaceName), methodConfigurationNamespace, methodConfigName) }
        }
      } ~
      path("workspaces" / Segment / Segment / "methodconfigs" / Segment / Segment / "rename") { (workspaceNamespace, workspaceName, methodConfigurationNamespace, methodConfigurationName) =>
        post {
          entity(as[MethodConfigurationName]) { newName =>
            complete { workspaceServiceConstructor(userInfo).RenameMethodConfiguration(WorkspaceName(workspaceNamespace, workspaceName), methodConfigurationNamespace, methodConfigurationName, newName) }
          }
        }
      } ~
      path("methodconfigs" / "copy") {
        post {
          entity(as[MethodConfigurationNamePair]) { confNames =>
            onSuccess(workspaceServiceConstructor(userInfo).CopyMethodConfiguration(confNames)) { validatedMethodConfig =>
              addLocationHeader(validatedMethodConfig.methodConfiguration.path(confNames.destination.workspaceName)) {
                complete {
                  StatusCodes.Created -> validatedMethodConfig
                }
              }
            }
          }
        }
      } ~
      path("methodconfigs" / "copyFromMethodRepo") {
        post {
          entity(as[MethodRepoConfigurationImport]) { query =>
            onSuccess(workspaceServiceConstructor(userInfo).CopyMethodConfigurationFromMethodRepo(query)) { validatedMethodConfig =>
              addLocationHeader(validatedMethodConfig.methodConfiguration.path(query.destination.workspaceName)) {
                complete {
                  StatusCodes.Created -> validatedMethodConfig
                }
              }
            }
          }
        }
      } ~
      path("methodconfigs" / "copyToMethodRepo") {
        post {
          entity(as[MethodRepoConfigurationExport]) { query =>
            complete { workspaceServiceConstructor(userInfo).CopyMethodConfigurationToMethodRepo(query) }
          }
        }
      } ~
      path("methodconfigs" / "template") {
        post {
          entity(as[MethodRepoMethod]) { methodRepoMethod =>
            complete { workspaceServiceConstructor(userInfo).CreateMethodConfigurationTemplate(methodRepoMethod) }
          }
        }
      } ~
      path("methodconfigs" / "inputsOutputs") {
        post {
          entity(as[MethodRepoMethod]) { methodRepoMethod =>
            complete { workspaceServiceConstructor(userInfo).GetMethodInputsOutputs(methodRepoMethod) }
          }
        }
      }
  }
}