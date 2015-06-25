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
@Api(value = "/{workspaceNamespace}/{workspaceName}/entities", description = "Entity manipulation API", position = 2)
trait EntityApiService extends HttpService with PerRequestCreator {
  import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
  import spray.httpx.SprayJsonSupport._

  val workspaceServiceConstructor: () => WorkspaceService
  val entityRoutes =
    createEntityRoute ~
    getEntityRoute ~
    updateEntityRoute ~
    deleteEntityRoute ~
    renameEntityRoute ~
    evaluateExpressionRoute ~
    listEntityTypesRoute ~
    listEntitiesPerTypeRoute

  @Path("")
  @ApiOperation(value = "Create entity in a workspace",
    nickname = "create entity",
    httpMethod = "POST",
    produces = "application/json",
    response = classOf[Entity])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "workspaceNamespace", required = true, dataType = "string", paramType = "path", value = "Workspace Namespace"),
    new ApiImplicitParam(name = "workspaceName", required = true, dataType = "string", paramType = "path", value = "Workspace Name"),
    new ApiImplicitParam(name = "entityJson", required = true, dataType = "org.broadinstitute.dsde.rawls.model.Entity", paramType = "body", value = "Entity data")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 201, message = "Successful Request"),
    new ApiResponse(code = 404, message = "Workspace not found"),
    new ApiResponse(code = 409, message = "Entity already exists"),
    new ApiResponse(code = 500, message = "Rawls Internal Error")
  ))
  def createEntityRoute = cookie("iPlanetDirectoryPro") { securityTokenCookie =>
    path("workspaces" / Segment / Segment / "entities") { (workspaceNamespace, workspaceName) =>
      post {
        entity(as[Entity]) { entity =>
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor),
            WorkspaceService.CreateEntity(workspaceNamespace, workspaceName, entity))
        }
      }
    }
  }

  @Path("/{entityType}/{entityName}")
  @ApiOperation(value = "Get entity in a workspace",
    nickname = "get entity",
    httpMethod = "Get",
    produces = "application/json",
    response = classOf[Entity])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "workspaceNamespace", required = true, dataType = "string", paramType = "path", value = "Workspace Namespace"),
    new ApiImplicitParam(name = "workspaceName", required = true, dataType = "string", paramType = "path", value = "Workspace Name"),
    new ApiImplicitParam(name = "entityType", required = true, dataType = "string", paramType = "path", value = "Entity Type"),
    new ApiImplicitParam(name = "entityName", required = true, dataType = "string", paramType = "path", value = "Entity Name")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "Successful Request"),
    new ApiResponse(code = 404, message = "Workspace or Entity does not exist"),
    new ApiResponse(code = 500, message = "Rawls Internal Error")
  ))
  def getEntityRoute = cookie("iPlanetDirectoryPro") { securityTokenCookie =>
    path("workspaces" / Segment / Segment / "entities" / Segment / Segment) { (workspaceNamespace, workspaceName, entityType, entityName) =>
      get {
        requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor),
          WorkspaceService.GetEntity(workspaceNamespace, workspaceName, entityType, entityName))
      }
    }
  }

  @Path("/{entityType}/{entityName}")
  @ApiOperation(value = "Update entity in a workspace",
    nickname = "update entity",
    httpMethod = "Patch",
    produces = "application/json",
    response = classOf[Entity])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "workspaceNamespace", required = true, dataType = "string", paramType = "path", value = "Workspace Namespace"),
    new ApiImplicitParam(name = "workspaceName", required = true, dataType = "string", paramType = "path", value = "Workspace Name"),
    new ApiImplicitParam(name = "entityType", required = true, dataType = "string", paramType = "path", value = "Entity Type"),
    new ApiImplicitParam(name = "entityName", required = true, dataType = "string", paramType = "path", value = "Entity Name"),
    new ApiImplicitParam(name = "entityUpdateJson", required = true, dataType = "org.broadinstitute.dsde.rawls.workspace.AttributeUpdateOperations$AttributeUpdateOperation", paramType = "body", value = "Update operations")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "Successful Request"),
    new ApiResponse(code = 400, message = "Attribute does not exists or is of an unexpected type"),
    new ApiResponse(code = 404, message = "Workspace or Entity does not exist"),
    new ApiResponse(code = 500, message = "Rawls Internal Error")
  ))
  def updateEntityRoute = cookie("iPlanetDirectoryPro") { securityTokenCookie =>
    path("workspaces" / Segment / Segment / "entities" / Segment / Segment) { (workspaceNamespace, workspaceName, entityType, entityName) =>
      patch {
        entity(as[Array[AttributeUpdateOperation]]) { operations =>
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor),
            WorkspaceService.UpdateEntity(workspaceNamespace, workspaceName, entityType, entityName, operations))
        }
      }
    }
  }

  @Path("/{entityType}/{entityName}")
  @ApiOperation(value = "delete entity in a workspace",
    nickname = "delete entity",
    httpMethod = "Delete")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "workspaceNamespace", required = true, dataType = "string", paramType = "path", value = "Workspace Namespace"),
    new ApiImplicitParam(name = "workspaceName", required = true, dataType = "string", paramType = "path", value = "Workspace Name"),
    new ApiImplicitParam(name = "entityType", required = true, dataType = "string", paramType = "path", value = "Entity Type"),
    new ApiImplicitParam(name = "entityName", required = true, dataType = "string", paramType = "path", value = "Entity Name")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 204, message = "Successful Request"),
    new ApiResponse(code = 404, message = "Workspace or Entity does not exist"),
    new ApiResponse(code = 500, message = "Rawls Internal Error")
  ))
  def deleteEntityRoute = cookie("iPlanetDirectoryPro") { securityTokenCookie =>
    path("workspaces" / Segment / Segment / "entities" / Segment / Segment) { (workspaceNamespace, workspaceName, entityType, entityName) =>
      delete {
        requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor),
          WorkspaceService.DeleteEntity(workspaceNamespace, workspaceName, entityType, entityName))
      }
    }
  }

  @Path("/{entityType}/{entityName}/rename")
  @ApiOperation(value = "rename entity in a workspace",
    nickname = "renameEntity",
    httpMethod = "Post")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "workspaceNamespace", required = true, dataType = "string", paramType = "path", value = "Workspace Namespace"),
    new ApiImplicitParam(name = "workspaceName", required = true, dataType = "string", paramType = "path", value = "Workspace Name"),
    new ApiImplicitParam(name = "entityType", required = true, dataType = "string", paramType = "path", value = "Entity Type"),
    new ApiImplicitParam(name = "entityName", required = true, dataType = "string", paramType = "path", value = "Entity Name"),
    new ApiImplicitParam(name = "newEntityNameJson", required = true, dataType = "org.broadinstitute.dsde.rawls.model.EntityName", paramType = "body", value = "New entity name")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 204, message = "Successful Request"),
    new ApiResponse(code = 404, message = "Workspace or Entity does not exist"),
    new ApiResponse(code = 500, message = "Rawls Internal Error")
  ))
  def renameEntityRoute = cookie("iPlanetDirectoryPro") { securityTokenCookie =>
    path("workspaces" / Segment / Segment / "entities" / Segment / Segment / "rename") { (workspaceNamespace, workspaceName, entityType, entityName) =>
      post {
        entity(as[EntityName]) { newEntityName =>
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor),
            WorkspaceService.RenameEntity(workspaceNamespace, workspaceName, entityType, entityName, newEntityName.name))
        }
      }
    }
  }

  @Path("/{entityType}/{entityName}/evaluate")
  @ApiOperation(value = "evaluate expression on an entity",
    nickname = "evaluateExpression",
    httpMethod = "Post")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "workspaceNamespace", required = true, dataType = "string", paramType = "path", value = "Workspace Namespace"),
    new ApiImplicitParam(name = "workspaceName", required = true, dataType = "string", paramType = "path", value = "Workspace Name"),
    new ApiImplicitParam(name = "entityType", required = true, dataType = "string", paramType = "path", value = "Entity Type"),
    new ApiImplicitParam(name = "entityName", required = true, dataType = "string", paramType = "path", value = "Entity Name")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "Successful Request"),
    new ApiResponse(code = 400, message = "Invalid entity expression"),
    new ApiResponse(code = 404, message = "Workspace or Entity does not exist"),
    new ApiResponse(code = 500, message = "Rawls Internal Error")
  ))
  def evaluateExpressionRoute = cookie("iPlanetDirectoryPro") { securityTokenCookie =>
    path("workspaces" / Segment / Segment / "entities" / Segment / Segment / "evaluate") { (workspaceNamespace, workspaceName, entityType, entityName) =>
      post {
        entity(as[String]) { expression =>
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor),
            WorkspaceService.EvaluateExpression(workspaceNamespace, workspaceName, entityType, entityName, expression))
        }
      }
    }
  }

  @Path("")
  @ApiOperation(value = "list all entity types in a workspace",
    nickname = "list entity types",
    httpMethod = "Get",
    produces = "application/json",
    response = classOf[Array[String]])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "workspaceNamespace", required = true, dataType = "string", paramType = "path", value = "Workspace Namespace"),
    new ApiImplicitParam(name = "workspaceName", required = true, dataType = "string", paramType = "path", value = "Workspace Name")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "Successful Request"),
    new ApiResponse(code = 404, message = "Workspace does not exist"),
    new ApiResponse(code = 500, message = "Rawls Internal Error")
  ))
  def listEntityTypesRoute = cookie("iPlanetDirectoryPro") { securityTokenCookie =>
    path("workspaces" / Segment / Segment / "entities") { (workspaceNamespace, workspaceName) =>
      get {
        requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor),
          WorkspaceService.ListEntityTypes(workspaceNamespace, workspaceName))
      }
    }
  }

  @Path("/{entityType}")
  @ApiOperation(value = "list all entities of given type in a workspace",
    nickname = "list entities",
    httpMethod = "Get",
    produces = "application/json",
    response = classOf[Array[Entity]])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "workspaceNamespace", required = true, dataType = "string", paramType = "path", value = "Workspace Namespace"),
    new ApiImplicitParam(name = "workspaceName", required = true, dataType = "string", paramType = "path", value = "Workspace Name"),
    new ApiImplicitParam(name = "entityType", required = true, dataType = "string", paramType = "path", value = "Entity Type")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "Successful Request"),
    new ApiResponse(code = 404, message = "Workspace or entityType does not exist"),
    new ApiResponse(code = 500, message = "Rawls Internal Error")
  ))
  def listEntitiesPerTypeRoute = cookie("iPlanetDirectoryPro") { securityTokenCookie =>
    path("workspaces" / Segment / Segment / "entities" / Segment) { (workspaceNamespace, workspaceName, entityType) =>
      get {
        requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor),
          WorkspaceService.ListEntities(workspaceNamespace, workspaceName, entityType))
      }
    }
  }

  @Path("/copy")
  @ApiOperation(value = "copy entities into a workspace from another workspace",
    nickname = "copy entities",
    httpMethod = "Post")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "entityCopyDefinition", required = true, dataType = "org.broadinstitute.dsde.rawls.model.EntityCopyDefinition", paramType = "body", value = "Source and destination for entities")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 201, message = "Successful Request"),
    new ApiResponse(code = 404, message = "Source Workspace or source entities does not exist"),
    new ApiResponse(code = 409, message = "One or more entities of that name already exist"),
    new ApiResponse(code = 500, message = "Rawls Internal Error")
  ))
  def copyEntitiesRoute = cookie("iPlanetDirectoryPro") { securityTokenCookie =>
    path("entities" / "copy" ) {
      post {
        entity(as[EntityCopyDefinition]) { copyDefinition =>
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor),
            WorkspaceService.CopyEntities(copyDefinition, requestContext.request.uri))
        }
      }
    }
  }
}
