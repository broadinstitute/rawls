package org.broadinstitute.dsde.rawls.webservice

import javax.ws.rs.Path

import com.wordnik.swagger.annotations._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.OpenAmDirectives
import org.broadinstitute.dsde.rawls.workspace.AttributeUpdateOperations.AttributeUpdateOperation
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import spray.json.JsonParser
import spray.routing.Directive.pimpApply
import spray.routing._

/**
 * Created by dvoet on 6/4/15.
 */
@Api(value = "workspaces", description = "Workspace manipulation API", position = 0)
trait WorkspaceApiService extends HttpService with PerRequestCreator with OpenAmDirectives {
  lazy private implicit val executionContext = actorRefFactory.dispatcher

  import spray.httpx.SprayJsonSupport._
  import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._

  val workspaceServiceConstructor: () => WorkspaceService
  val workspaceRoutes =
    postWorkspaceRoute ~
    getWorkspacesRoute ~
    updateWorkspaceRoute ~
    listWorkspacesRoute ~
    copyWorkspaceRoute ~
    getACLRoute ~
    putACLRoute

  @ApiOperation(value = "Create/replace workspace",
    nickname = "create",
    httpMethod = "POST")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "workspaceJson", required = true, dataType = "org.broadinstitute.dsde.rawls.model.WorkspaceRequest", paramType = "body", value = "Workspace contents")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 201, message = "Successful Request"),
    new ApiResponse(code = 500, message = "Rawls Internal Error")
  ))
  def postWorkspaceRoute = usernameFromCookie() { userId =>
    path("workspaces") {
      post {
        entity(as[WorkspaceRequest]) { workspace =>
          requestContext => perRequest(requestContext,
            WorkspaceService.props(workspaceServiceConstructor),
            WorkspaceService.CreateWorkspace(userId,workspace))
        }
      }
    }
  }

  @Path("/{workspaceNamespace}/{workspaceName}")
  @ApiOperation(value = "Update attributes of a workspace",
    nickname = "update workspace",
    httpMethod = "Patch",
    produces = "application/json",
    response = classOf[Workspace])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "workspaceNamespace", required = true, dataType = "string", paramType = "path", value = "Workspace Namespace"),
    new ApiImplicitParam(name = "workspaceName", required = true, dataType = "string", paramType = "path", value = "Workspace Name"),
    new ApiImplicitParam(name = "entityUpdateJson", required = true, dataType = "org.broadinstitute.dsde.rawls.workspace.AttributeUpdateOperations$AttributeUpdateOperation", paramType = "body", value = "Update operations")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "Successful Request"),
    new ApiResponse(code = 400, message = "Attribute does not exists or is of an unexpected type"),
    new ApiResponse(code = 404, message = "Workspace does not exists"),
    new ApiResponse(code = 500, message = "Rawls Internal Error")
  ))
  def updateWorkspaceRoute = cookie("iPlanetDirectoryPro") { securityTokenCookie =>
    path("workspaces" / Segment / Segment) { (workspaceNamespace, workspaceName) =>
      patch {
        entity(as[Array[AttributeUpdateOperation]]) { operations =>
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor),
            WorkspaceService.UpdateWorkspace(workspaceNamespace, workspaceName, operations))
        }
      }
    }
  }

  @Path("/{workspaceNamespace}/{workspaceName}")
  @ApiOperation(value = "Get workspace",
    nickname = "list",
    httpMethod = "GET",
    produces = "application/json",
    response = classOf[Workspace])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "workspaceNamespace", required = true, dataType = "string", paramType = "path", value = "Workspace Namespace"),
    new ApiImplicitParam(name = "workspaceName", required = true, dataType = "string", paramType = "path", value = "Workspace Name")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "Successful Request"),
    new ApiResponse(code = 404, message = "Workspace does not exists"),
    new ApiResponse(code = 500, message = "Rawls Internal Error")
  ))
  def getWorkspacesRoute = cookie("iPlanetDirectoryPro") { securityTokenCookie =>
    path("workspaces" / Segment / Segment) { (workspaceNamespace, workspaceName) =>
      get {
        requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor), WorkspaceService.GetWorkspace(workspaceNamespace, workspaceName))
      }
    }
  }

  @ApiOperation(value = "List workspaces",
    nickname = "list",
    httpMethod = "GET",
    produces = "application/json",
    response = classOf[Seq[Workspace]])
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "Successful Request"),
    new ApiResponse(code = 500, message = "Rawls Internal Error")
  ))
  def listWorkspacesRoute = cookie("iPlanetDirectoryPro") { securityTokenCookie =>
    path("workspaces") {
      get {
        requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor), WorkspaceService.ListWorkspaces)
      }
    }
  }

  @Path("/{workspaceNamespace}/{workspaceName}/clone")
  @ApiOperation(value = "Clone workspace",
    nickname = "clone",
    httpMethod = "POST")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "workspaceNamespace", required = true, dataType = "string", paramType = "path", value = "Workspace Namespace"),
    new ApiImplicitParam(name = "workspaceName", required = true, dataType = "string", paramType = "path", value = "Workspace Name"),
    new ApiImplicitParam(name = "workspaceNameJson", required = true, dataType = "org.broadinstitute.dsde.rawls.model.WorkspaceName", paramType = "body", value = "Name of new workspace")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 201, message = "Successful Request"),
    new ApiResponse(code = 404, message = "Source workspace not found"),
    new ApiResponse(code = 409, message = "Destination workspace already exists"),
    new ApiResponse(code = 500, message = "Rawls Internal Error")
  ))
  def copyWorkspaceRoute = usernameFromCookie() { userId =>
    path("workspaces" / Segment / Segment / "clone" ) { (sourceNamespace, sourceWorkspace) =>
      post {
        entity(as[WorkspaceName]) { destWorkspace =>
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor),
            WorkspaceService.CloneWorkspace(userId, sourceNamespace, sourceWorkspace, destWorkspace.namespace, destWorkspace.name))
        }
      }
    }
  }

  @Path("/{workspaceNamespace}/{workspaceName}/acl")
  @ApiOperation(value = "Get workspace ACL",
    nickname = "getACL",
    httpMethod = "GET",
    produces = "application/json",
    response = classOf[String])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "workspaceNamespace", required = true, dataType = "string", paramType = "path", value = "Workspace Namespace"),
    new ApiImplicitParam(name = "workspaceName", required = true, dataType = "string", paramType = "path", value = "Workspace Name")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "Successful Request"),
    new ApiResponse(code = 404, message = "Workspace not found"),
    new ApiResponse(code = 500, message = "Rawls Internal Error")
  ))
  def getACLRoute = usernameFromCookie() { userId =>
    path("workspaces" / Segment / Segment / "acl" ) { (workspaceNamespace, workspaceName) =>
      get {
        requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor),
                                WorkspaceService.GetACL(userId, workspaceNamespace, workspaceName))
      }
    }
  }

  @Path("/{workspaceNamespace}/{workspaceName}/acl")
  @ApiOperation(value = "Set workspace ACL",
    nickname = "putACL",
    httpMethod = "PUT")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "workspaceNamespace", required = true, dataType = "string", paramType = "path", value = "Workspace Namespace"),
    new ApiImplicitParam(name = "workspaceName", required = true, dataType = "string", paramType = "path", value = "Workspace Name"),
    new ApiImplicitParam(name = "acl", required = true, dataType = "String", paramType = "body", value = "New ACL for Workspace")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "Successful Request"),
    new ApiResponse(code = 404, message = "Workspace not found"),
    new ApiResponse(code = 500, message = "Rawls Internal Error")
  ))
  def putACLRoute = cookie("iPlanetDirectoryPro") { securityTokenCookie =>
    path("workspaces" / Segment / Segment / "acl" ) { (workspaceNamespace, workspaceName) =>
      put {
        usernameFromCookie() { userId =>
          entity(as[String]) { acl =>
            requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor),
                                      WorkspaceService.PutACL(userId, workspaceNamespace, workspaceName, acl))
          }
        }
      }
    }
  }
}
