package org.broadinstitute.dsde.rawls.webservice

import javax.ws.rs.Path

import akka.actor.{Actor, ActorRefFactory, Props}
import com.gettyimages.spray.swagger.SwaggerHttpService
import com.wordnik.swagger.annotations._
import com.wordnik.swagger.model.ApiInfo
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.workspace.EntityUpdateOperations.EntityUpdateOperation
import org.broadinstitute.dsde.rawls.workspace.{EntityUpdateOperations, WorkspaceService}
import spray.http.MediaTypes._
import spray.http.Uri
import spray.routing.Directive.pimpApply
import spray.routing._

import scala.reflect.runtime.universe._

object RawlsApiServiceActor {
  def props(swaggerService: SwaggerService, workspaceServiceConstructor: () => WorkspaceService): Props = {
    Props(new RawlsApiServiceActor(swaggerService, workspaceServiceConstructor))
  }
}

class SwaggerService(override val apiVersion: String,
                     override val baseUrl: String,
                     override val docsPath: String,
                     override val swaggerVersion: String,
                     override val apiTypes: Seq[Type],
                     override val apiInfo: Option[ApiInfo])
  (implicit val actorRefFactory: ActorRefFactory)
  extends SwaggerHttpService

class RawlsApiServiceActor(swaggerService: SwaggerService, val workspaceServiceConstructor: () => WorkspaceService) extends Actor with RootRawlsApiService with WorkspaceApiService {
  implicit def executionContext = actorRefFactory.dispatcher
  def actorRefFactory = context
  def possibleRoutes = baseRoute ~ workspaceRoutes ~ swaggerService.routes ~
    get {
      pathSingleSlash {
        getFromResource("swagger/index.html")
      } ~ getFromResourceDirectory("swagger/") ~ getFromResourceDirectory("META-INF/resources/webjars/swagger-ui/2.0.24/")
    }

  def receive = runRoute(possibleRoutes)
}

@Api(value = "", description = "Rawls Base API", position = 1)
trait RootRawlsApiService extends HttpService {
  @ApiOperation(value = "Check if Rawls is alive",
    nickname = "poke",
    httpMethod = "GET",
    produces = "text/html",
    response = classOf[String])
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "Successful Request"),
    new ApiResponse(code = 500, message = "Rawls Internal Error")
  ))
  def baseRoute = {
    path("headers") {
      get {
        requestContext => requestContext.complete(requestContext.request.headers.mkString(",\n"))
      }
    }
  }
}

@Api(value = "workspaces", description = "APIs for Workspace CRUD", position = 1)
trait WorkspaceApiService extends HttpService with PerRequestCreator {
  import spray.httpx.SprayJsonSupport._
  import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._

  val workspaceServiceConstructor: () => WorkspaceService
  val workspaceRoutes =
    postWorkspaceRoute ~
    listWorkspacesRoute ~
    copyWorkspaceRoute ~
    createEntityRoute ~
    getEntityRoute ~
    updateEntityRoute ~
    deleteEntityRoute ~
    renameEntityRoute ~
    createMethodConfigurationRoute ~
    deleteMethodConfigurationRoute ~
    renameMethodConfigurationRoute ~
    updateMethodConfigurationRoute

  @ApiOperation(value = "Create/replace workspace",
    nickname = "create",
    httpMethod = "POST")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "workspaceJson", required = true, dataType = "org.broadinstitute.dsde.rawls.model.Workspace", paramType = "body", value = "Workspace contents")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 201, message = "Successful Request"),
    new ApiResponse(code = 500, message = "Rawls Internal Error")
  ))
  def postWorkspaceRoute = cookie("iPlanetDirectoryPro") { securityTokenCookie =>
    path("workspaces") {
      post {
        entity(as[Workspace]) { workspace =>
          requestContext => perRequest(requestContext,
            WorkspaceService.props(workspaceServiceConstructor),
            WorkspaceService.SaveWorkspace(workspace))
        }
      }
    }
  }

  @ApiOperation(value = "List workspaces",
    nickname = "list",
    httpMethod = "GET",
    produces = "application/json",
    response = classOf[Seq[WorkspaceShort]])
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
  def copyWorkspaceRoute = cookie("iPlanetDirectoryPro") { securityTokenCookie =>
    path("workspaces" / Segment / Segment / "clone" ) { (sourceNamespace, sourceWorkspace) =>
      post {
        entity(as[WorkspaceName]) { destWorkspace =>
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor),
            WorkspaceService.CloneWorkspace(sourceNamespace, sourceWorkspace, destWorkspace.namespace, destWorkspace.name))
        }
      }
    }
  }

  @Path("/{workspaceNamespace}/{workspaceName}/entities")
  @ApiOperation(value = "Create entity in a workspace",
    nickname = "create entity",
    httpMethod = "POST",
    produces = "application/json",
    response = classOf[Entity])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "workspaceNamespace", required = true, dataType = "string", paramType = "path", value = "Workspace Namespace"),
    new ApiImplicitParam(name = "workspaceName", required = true, dataType = "string", paramType = "path", value = "Workspace Name"),
    new ApiImplicitParam(name = "entityType", required = true, dataType = "string", paramType = "path", value = "Entity Type"),
    new ApiImplicitParam(name = "entityName", required = true, dataType = "string", paramType = "path", value = "Entity Name"),
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

  @Path("/{workspaceNamespace}/{workspaceName}/entities/{entityType}/{entityName}")
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
    new ApiResponse(code = 404, message = "Workspace or Entity does not exists"),
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

  @Path("/{workspaceNamespace}/{workspaceName}/entities/{entityType}/{entityName}")
  @ApiOperation(value = "Update entity in a workspace",
    nickname = "update entity",
    httpMethod = "Post",
    produces = "application/json",
    response = classOf[Entity])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "workspaceNamespace", required = true, dataType = "string", paramType = "path", value = "Workspace Namespace"),
    new ApiImplicitParam(name = "workspaceName", required = true, dataType = "string", paramType = "path", value = "Workspace Name"),
    new ApiImplicitParam(name = "entityType", required = true, dataType = "string", paramType = "path", value = "Entity Type"),
    new ApiImplicitParam(name = "entityName", required = true, dataType = "string", paramType = "path", value = "Entity Name"),
    new ApiImplicitParam(name = "entityUpdateJson", required = true, dataType = "org.broadinstitute.dsde.rawls.workspace.EntityUpdateOperations$EntityUpdateOperation", paramType = "body", value = "Update operations")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "Successful Request"),
    new ApiResponse(code = 400, message = "Attribute does not exists or is of an unexpected type"),
    new ApiResponse(code = 404, message = "Workspace or Entity does not exists"),
    new ApiResponse(code = 500, message = "Rawls Internal Error")
  ))
  def updateEntityRoute = cookie("iPlanetDirectoryPro") { securityTokenCookie =>
    import EntityUpdateOperations._
    path("workspaces" / Segment / Segment / "entities" / Segment / Segment) { (workspaceNamespace, workspaceName, entityType, entityName) =>
      post {
        entity(as[Array[EntityUpdateOperation]]) { operations =>
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor),
            WorkspaceService.UpdateEntity(workspaceNamespace, workspaceName, entityType, entityName, operations))
        }
      }
    }
  }

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
    new ApiResponse(code = 404, message = "Workspace or Entity does not exists"),
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
    new ApiResponse(code = 404, message = "Workspace or Entity does not exists"),
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

  @Path("/{workspaceNamespace}/{workspaceName}/methodconfigs")
  @ApiOperation(value = "Create Method configuration in a workspace",
    nickname = "create method configuration",
    httpMethod = "POST",
    produces = "application/json",
    response = classOf[MethodConfiguration])
  @ApiImplicitParams(Array(
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

  @Path("/{workspaceNamespace}/{workspaceName}/methodconfigs")
  @ApiOperation(value = "delete method configuration in a workspace",
    nickname = "delete method configuration",
    httpMethod = "Delete")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "workspaceNamespace", required = true, dataType = "string", paramType = "path", value = "Workspace Namespace"),
    new ApiImplicitParam(name = "workspaceName", required = true, dataType = "string", paramType = "path", value = "Workspace Name"),
    new ApiImplicitParam(name = "methodConfigurationName", required = true, dataType = "string", paramType = "path", value = "Method Configuration Name")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 204, message = "Successful Request"),
    new ApiResponse(code = 404, message = "Workspace or Method Configuration does not exist"),
    new ApiResponse(code = 500, message = "Rawls Internal Error")
  ))
  def deleteMethodConfigurationRoute = cookie("iPlanetDirectoryPro") { securityTokenCookie =>
    path("workspaces" / Segment / Segment / "methodconfigs" / Segment) { (workspaceNamespace, workspaceName, methodConfigName) =>
      delete {
        requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor),
          WorkspaceService.DeleteMethodConfiguration(workspaceNamespace, workspaceName, methodConfigName))
      }
    }
  }

  @Path("/{workspaceNamespace}/{workspaceName}/methodconfigs")
  @ApiOperation(value = "rename method configuration in a workspace",
    nickname = "renamemethodconfig",
    httpMethod = "Post")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "workspaceNamespace", required = true, dataType = "string", paramType = "path", value = "Workspace Namespace"),
    new ApiImplicitParam(name = "workspaceName", required = true, dataType = "string", paramType = "path", value = "Workspace Name"),
    new ApiImplicitParam(name = "methodConfigurationName", required = true, dataType = "string", paramType = "path", value = "Method Configuration Name"),
    new ApiImplicitParam(name = "newMethodConfigurationName", required = true, dataType = "string", paramType = "path", value = "New Method Configuration Name")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 204, message = "Successful Request"),
    new ApiResponse(code = 404, message = "Workspace or Method Configuration does not exists"),
    new ApiResponse(code = 500, message = "Rawls Internal Error")
  ))
  def renameMethodConfigurationRoute = cookie("iPlanetDirectoryPro") { securityTokenCookie =>
    path("workspaces" / Segment / Segment / "methodconfigs" / Segment / "rename") { (workspaceNamespace, workspaceName, methodConfigurationName) =>
      post {
        entity(as[MethodConfigurationName]) { newEntityName =>
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor),
            WorkspaceService.RenameMethodConfiguration(workspaceNamespace, workspaceName, methodConfigurationName, newEntityName.name))
        }
      }
    }
  }

  @Path("/{workspaceNamespace}/{workspaceName}/methodconfigs")
  @ApiOperation(value = "Update method configuration in a workspace",
    nickname = "update method configuration",
    httpMethod = "Post",
    produces = "application/json",
    response = classOf[MethodConfiguration])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "workspaceNamespace", required = true, dataType = "string", paramType = "path", value = "Workspace Namespace"),
    new ApiImplicitParam(name = "workspaceName", required = true, dataType = "string", paramType = "path", value = "Workspace Name"),
    new ApiImplicitParam(name = "methodConfigurationName", required = true, dataType = "string", paramType = "path", value = "Method Configuration Name"),
    new ApiImplicitParam(name = "newMethodConfigJson", required = true, dataType = "org.broadinstitute.dsde.rawls.model.MethodConfiguration", paramType = "body", value = "New Method Configuration contents")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "Successful Request"),
    new ApiResponse(code = 404, message = "Workspace or method configuration does not exists"),
    new ApiResponse(code = 500, message = "Rawls Internal Error")
  ))
  def updateMethodConfigurationRoute = cookie("iPlanetDirectoryPro") { securityTokenCookie =>
    path("workspaces" / Segment / Segment / "methodconfigs" / Segment ) { (workspaceNamespace, workspaceName, methodConfigurationName) =>
      post {
        entity(as[MethodConfiguration]) { newMethodConfiguration =>
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor),
            WorkspaceService.UpdateMethodConfiguration(workspaceNamespace, workspaceName, newMethodConfiguration))
        }
      }
    }
  }
}
