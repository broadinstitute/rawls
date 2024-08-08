package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import io.opentelemetry.context.Context
import org.broadinstitute.dsde.rawls.methods.MethodConfigurationService
import org.broadinstitute.dsde.rawls.model.MethodRepoJsonSupport.AgoraEntityFormat
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.webservice.CustomDirectives._
import spray.json.DefaultJsonProtocol._

import scala.concurrent.ExecutionContext

/**
  * Created by dvoet on 6/4/15.
  */

trait MethodConfigApiService extends UserInfoDirectives {
  implicit val executionContext: ExecutionContext

  val methodConfigurationServiceConstructor: RawlsRequestContext => MethodConfigurationService

  def methodConfigRoutes(otelContext: Context = Context.root()): server.Route = {
    requireUserInfo(Option(otelContext)) { userInfo =>
      val ctx = RawlsRequestContext(userInfo, Option(otelContext))
      path("workspaces" / Segment / Segment / "methodconfigs") { (workspaceNamespace, workspaceName) =>
        get {
          parameters("allRepos".as[Boolean] ? false) { allRepos =>
            if (allRepos) {
              complete {
                methodConfigurationServiceConstructor(ctx).listMethodConfigurations(
                  WorkspaceName(workspaceNamespace, workspaceName)
                )
              }
            } else {
              complete {
                methodConfigurationServiceConstructor(ctx).listAgoraMethodConfigurations(
                  WorkspaceName(workspaceNamespace, workspaceName)
                )
              }
            }
          }
        } ~
          post {
            entity(as[MethodConfiguration]) { methodConfiguration =>
              addLocationHeader(methodConfiguration.path(WorkspaceName(workspaceNamespace, workspaceName))) {
                complete {
                  methodConfigurationServiceConstructor(ctx)
                    .createMethodConfiguration(WorkspaceName(workspaceNamespace, workspaceName), methodConfiguration)
                    .map(StatusCodes.Created -> _)
                }
              }
            }
          }
      } ~
        path("workspaces" / Segment / Segment / "methodconfigs" / Segment / Segment) {
          (workspaceNamespace, workspaceName, methodConfigurationNamespace, methodConfigName) =>
            get {
              complete {
                methodConfigurationServiceConstructor(ctx).getMethodConfiguration(WorkspaceName(workspaceNamespace,
                                                                                                workspaceName
                                                                                  ),
                                                                                  methodConfigurationNamespace,
                                                                                  methodConfigName
                )
              }
            } ~
              put {
                entity(as[MethodConfiguration]) { newMethodConfiguration =>
                  addLocationHeader(newMethodConfiguration.path(WorkspaceName(workspaceNamespace, workspaceName))) {
                    complete {
                      methodConfigurationServiceConstructor(ctx).overwriteMethodConfiguration(
                        WorkspaceName(workspaceNamespace, workspaceName),
                        methodConfigurationNamespace,
                        methodConfigName,
                        newMethodConfiguration
                      )
                    }
                  }
                }
              } ~
              post {
                entity(as[MethodConfiguration]) { newMethodConfiguration =>
                  complete {
                    methodConfigurationServiceConstructor(ctx).updateMethodConfiguration(
                      WorkspaceName(workspaceNamespace, workspaceName),
                      methodConfigurationNamespace,
                      methodConfigName,
                      newMethodConfiguration
                    )
                  }
                }
              } ~
              delete {
                complete {
                  methodConfigurationServiceConstructor(ctx)
                    .deleteMethodConfiguration(WorkspaceName(workspaceNamespace, workspaceName),
                                               methodConfigurationNamespace,
                                               methodConfigName
                    )
                    .map(_ => StatusCodes.NoContent)
                }
              }
        } ~
        path("workspaces" / Segment / Segment / "methodconfigs" / Segment / Segment / "validate") {
          (workspaceNamespace, workspaceName, methodConfigurationNamespace, methodConfigName) =>
            get {
              complete {
                methodConfigurationServiceConstructor(ctx).getAndValidateMethodConfiguration(
                  WorkspaceName(workspaceNamespace, workspaceName),
                  methodConfigurationNamespace,
                  methodConfigName
                )
              }
            }
        } ~
        path("workspaces" / Segment / Segment / "methodconfigs" / Segment / Segment / "rename") {
          (workspaceNamespace, workspaceName, methodConfigurationNamespace, methodConfigurationName) =>
            post {
              entity(as[MethodConfigurationName]) { newName =>
                complete {
                  methodConfigurationServiceConstructor(ctx)
                    .renameMethodConfiguration(WorkspaceName(workspaceNamespace, workspaceName),
                                               methodConfigurationNamespace,
                                               methodConfigurationName,
                                               newName
                    )
                    .map(_ => StatusCodes.NoContent)
                }
              }
            }
        } ~
        path("methodconfigs" / "copy") {
          post {
            entity(as[MethodConfigurationNamePair]) { confNames =>
              onSuccess(methodConfigurationServiceConstructor(ctx).copyMethodConfiguration(confNames)) {
                validatedMethodConfig =>
                  addLocationHeader(
                    validatedMethodConfig.methodConfiguration.path(confNames.destination.workspaceName)
                  ) {
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
              onSuccess(methodConfigurationServiceConstructor(ctx).copyMethodConfigurationFromMethodRepo(query)) {
                validatedMethodConfig =>
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
              complete {
                methodConfigurationServiceConstructor(ctx).copyMethodConfigurationToMethodRepo(query)
              }
            }
          }
        } ~
        path("methodconfigs" / "template") {
          post {
            entity(as[MethodRepoMethod]) { methodRepoMethod =>
              complete {
                methodConfigurationServiceConstructor(ctx).createMethodConfigurationTemplate(methodRepoMethod)
              }
            }
          }
        } ~
        path("methodconfigs" / "inputsOutputs") {
          post {
            entity(as[MethodRepoMethod]) { methodRepoMethod =>
              complete {
                methodConfigurationServiceConstructor(ctx).getMethodInputsOutputs(userInfo, methodRepoMethod)
              }
            }
          }
        }
    }
  }
}
