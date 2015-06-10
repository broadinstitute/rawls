package org.broadinstitute.dsde.rawls.webservice

import javax.ws.rs.Path

import com.wordnik.swagger.annotations._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.workspace.AttributeUpdateOperations.AttributeUpdateOperation
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import spray.routing.Directive.pimpApply
import spray.routing._

/**
 * Created by dvoet on 6/4/15.
 */
@Api(value = "/{workspaceNamespace}/{workspaceName}/methodconfigs", description = "Method Configuration manipulation API", position = 3)
trait MethodConfigApiService extends HttpService with PerRequestCreator {
  import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
  import spray.httpx.SprayJsonSupport._

  val workspaceServiceConstructor: () => WorkspaceService
  val methodConfigRoutes =
    createMethodConfigurationRoute ~
    getMethodConfigurationRoute ~
    deleteMethodConfigurationRoute ~
    renameMethodConfigurationRoute ~
    updateMethodConfigurationRoute ~
    copyMethodConfigurationRoute ~
    listMethodConfigurationsRoute

  @Path("")
  @ApiOperation(value = "Create Method configuration in a workspace",
    nickname = "create method configuration",
    httpMethod = "POST",
    produces = "application/json",
    response = classOf[MethodConfiguration])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "workspaceNamespace", required = true, dataType = "string", paramType = "path", value = "Workspace Namespace"),
    new ApiImplicitParam(name = "workspaceName", required = true, dataType = "string", paramType = "path", value = "Workspace Name"),
    new ApiImplicitParam(name = "methodConfigJson", required = true, dataType = "org.broadinstitute.dsde.rawls.model.MethodConfiguration", paramType = "body", value = "Method Configuration contents")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 201, message = "Successful Request"),
    new ApiResponse(code = 404, message = "Workspace not found"),
    new ApiResponse(code = 409, message = "MethodConfiguration already exists"),
    new ApiResponse(code = 500, message = "Rawls Internal Error")
  ))
  def createMethodConfigurationRoute = cookie("iPlanetDirectoryPro") { securityTokenCookie =>
    path("workspaces" / Segment / Segment / "methodconfigs") { (workspaceNamespace, workspaceName) =>
      post {
        entity(as[MethodConfiguration]) { methodConfiguration =>
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor),
            WorkspaceService.CreateMethodConfiguration(workspaceNamespace, workspaceName, methodConfiguration))
        }
      }
    }
  }

  @Path("/{configNamespace}/{configName}")
  @ApiOperation(value = "get method configuration in a workspace",
    nickname = "get method configuration",
    httpMethod = "GET",
    produces = "application/json",
    response = classOf[MethodConfiguration])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "workspaceNamespace", required = true, dataType = "string", paramType = "path", value = "Workspace Namespace"),
    new ApiImplicitParam(name = "workspaceName", required = true, dataType = "string", paramType = "path", value = "Workspace Name"),
    new ApiImplicitParam(name = "methodConfigurationNamespace", required = true, dataType = "string", paramType = "path", value = "Method Configuration Namespace"),
    new ApiImplicitParam(name = "methodConfigurationName", required = true, dataType = "string", paramType = "path", value = "Method Configuration Name")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 204, message = "Successful Request"),
    new ApiResponse(code = 404, message = "Workspace or Method Configuration does not exist"),
    new ApiResponse(code = 500, message = "Rawls Internal Error")
  ))
  def getMethodConfigurationRoute = cookie("iPlanetDirectoryPro") { securityTokenCookie =>
    path("workspaces" / Segment / Segment / "methodconfigs" / Segment / Segment) { (workspaceNamespace, workspaceName, methodConfigurationNamespace, methodConfigName) =>
      get {
        requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor),
          WorkspaceService.GetMethodConfiguration(workspaceNamespace, workspaceName, methodConfigurationNamespace, methodConfigName))
      }
    }
  }

  @Path("")
  @ApiOperation(value = "list method configurations in a workspace",
    nickname = "list method configurations",
    httpMethod = "GET",
    response = classOf[Seq[MethodConfigurationShort]]
  )
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "workspaceNamespace", required = true, dataType = "string", paramType = "path", value = "Workspace Namespace"),
    new ApiImplicitParam(name = "workspaceName", required = true, dataType = "string", paramType = "path", value = "Workspace Name")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 204, message = "Successful Request"),
    new ApiResponse(code = 404, message = "Workspace does not exist"),
    new ApiResponse(code = 500, message = "Rawls Internal Error")
  ))
  def listMethodConfigurationsRoute = cookie("iPlanetDirectoryPro") { securityTokenCookie =>
    path("workspaces" / Segment / Segment / "methodconfigs" ) { (workspaceNamespace, workspaceName) =>
      get {
        requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor),
          WorkspaceService.ListMethodConfigurations(workspaceNamespace, workspaceName))
      }
    }
  }

  @Path("/{configNamespace}/{configName}")
  @ApiOperation(value = "delete method configuration in a workspace",
    nickname = "delete method configuration",
    httpMethod = "Delete")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "workspaceNamespace", required = true, dataType = "string", paramType = "path", value = "Workspace Namespace"),
    new ApiImplicitParam(name = "workspaceName", required = true, dataType = "string", paramType = "path", value = "Workspace Name"),
    new ApiImplicitParam(name = "methodConfigurationNamespace", required = true, dataType = "string", paramType = "path", value = "Method Configuration Namespace"),
    new ApiImplicitParam(name = "methodConfigurationName", required = true, dataType = "string", paramType = "path", value = "Method Configuration Name")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 204, message = "Successful Request"),
    new ApiResponse(code = 404, message = "Workspace or Method Configuration does not exist"),
    new ApiResponse(code = 500, message = "Rawls Internal Error")
  ))
  def deleteMethodConfigurationRoute = cookie("iPlanetDirectoryPro") { securityTokenCookie =>
    path("workspaces" / Segment / Segment / "methodconfigs" / Segment / Segment) { (workspaceNamespace, workspaceName, methodConfigurationNamespace, methodConfigName) =>
      delete {
        requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor),
          WorkspaceService.DeleteMethodConfiguration(workspaceNamespace, workspaceName, methodConfigurationNamespace, methodConfigName))
      }
    }
  }

  @Path("/{configNamespace}/{configName}/rename")
  @ApiOperation(value = "rename method configuration in a workspace",
    nickname = "renamemethodconfig",
    httpMethod = "Post")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "workspaceNamespace", required = true, dataType = "string", paramType = "path", value = "Workspace Namespace"),
    new ApiImplicitParam(name = "workspaceName", required = true, dataType = "string", paramType = "path", value = "Workspace Name"),
    new ApiImplicitParam(name = "methodConfigurationNamespace", required = true, dataType = "string", paramType = "path", value = "Method Configuration Namespace"),
    new ApiImplicitParam(name = "methodConfigurationName", required = true, dataType = "string", paramType = "path", value = "Method Configuration Name"),
    new ApiImplicitParam(name = "newMethodConfigurationName", required = true, dataType = "org.broadinstitute.dsde.rawls.model.MethodConfigurationName", paramType = "body", value = "New Method Configuration Name")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 204, message = "Successful Request"),
    new ApiResponse(code = 404, message = "Workspace or Method Configuration does not exists"),
    new ApiResponse(code = 500, message = "Rawls Internal Error")
  ))
  def renameMethodConfigurationRoute = cookie("iPlanetDirectoryPro") { securityTokenCookie =>
    path("workspaces" / Segment / Segment / "methodconfigs" / Segment / Segment / "rename") { (workspaceNamespace, workspaceName, methodConfigurationNamespace, methodConfigurationName) =>
      post {
        entity(as[MethodConfigurationName]) { newEntityName =>
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor),
            WorkspaceService.RenameMethodConfiguration(workspaceNamespace, workspaceName, methodConfigurationNamespace, methodConfigurationName, newEntityName.name))
        }
      }
    }
  }

  @Path("/{configNamespace}/{configName}")
  @ApiOperation(value = "Update method configuration in a workspace",
    nickname = "update method configuration",
    httpMethod = "Put",
    produces = "application/json",
    response = classOf[MethodConfiguration])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "workspaceNamespace", required = true, dataType = "string", paramType = "path", value = "Workspace Namespace"),
    new ApiImplicitParam(name = "workspaceName", required = true, dataType = "string", paramType = "path", value = "Workspace Name"),
    new ApiImplicitParam(name = "methodConfigurationNamespace", required = true, dataType = "string", paramType = "path", value = "Method Configuration Namespace"),
    new ApiImplicitParam(name = "methodConfigurationName", required = true, dataType = "string", paramType = "path", value = "Method Configuration Name"),
    new ApiImplicitParam(name = "newMethodConfigJson", required = true, dataType = "org.broadinstitute.dsde.rawls.model.MethodConfiguration", paramType = "body", value = "New Method Configuration contents")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "Successful Request"),
    new ApiResponse(code = 404, message = "Workspace or method configuration does not exists"),
    new ApiResponse(code = 500, message = "Rawls Internal Error")
  ))
  def updateMethodConfigurationRoute = cookie("iPlanetDirectoryPro") { securityTokenCookie =>
    path("workspaces" / Segment / Segment / "methodconfigs" / Segment / Segment ) { (workspaceNamespace, workspaceName, methodConfigurationNamespace, methodConfigName) =>
      put {
        entity(as[MethodConfiguration]) { newMethodConfiguration =>
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor),
            WorkspaceService.UpdateMethodConfiguration(workspaceNamespace, workspaceName, newMethodConfiguration.copy(namespace = methodConfigurationNamespace, name = methodConfigName)))
        }
      }
    }
  }

  @Path("/methodconfigs/copy")
  @ApiOperation(value = "Copy method configuration in a workspace from another workspace",
    nickname = "copy method configuration",
    httpMethod = "Post",
    produces = "application/json",
    response = classOf[MethodConfiguration])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "methodConfigurationNamePair", required = true, dataType = "org.broadinstitute.dsde.rawls.model.MethodConfigurationNamePair", paramType = "body", value = "Source and destination method configuration names")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 201, message = "Successful Request"),
    new ApiResponse(code = 404, message = "Source Workspace or method configuration does not exist"),
    new ApiResponse(code = 409, message = "Destination method configuration by that name already exists"),
    new ApiResponse(code = 500, message = "Rawls Internal Error")
  ))
  def copyMethodConfigurationRoute = cookie("iPlanetDirectoryPro") { securityTokenCookie =>
    path("methodconfigs" / "copy" ) {
      post {
        entity(as[MethodConfigurationNamePair]) { confNames =>
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor),
            WorkspaceService.CopyMethodConfiguration(confNames))
        }
      }
    }
  }
}
