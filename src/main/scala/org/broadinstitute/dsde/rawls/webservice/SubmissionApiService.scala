package org.broadinstitute.dsde.rawls.webservice

import java.util.logging.Logger
import javax.ws.rs.Path
import com.wordnik.swagger.annotations._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport._
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import spray.httpx.SprayJsonSupport._
import spray.routing._

/**
 * Created by dvoet on 6/4/15.
 */
@Api(value = "/workspaces/{workspaceNamespace}/{workspaceName}/submissions", description = "Submissions API")
trait SubmissionApiService extends HttpService with PerRequestCreator {

  val workspaceServiceConstructor: () => WorkspaceService
  val submissionRoutes = submissionRoute

  @ApiOperation(value = "Create Submission.",
    nickname = "createSubmission",
    httpMethod = "POST",
    produces = "application/json",
    response = classOf[Submission])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "submission", required = true, dataType = "org.broadinstitute.dsde.rawls.model.SubmissionRequest", paramType = "body", value = "Description of a submission.")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 201, message = "Successful Request"),
    new ApiResponse(code = 404, message = "Method Configuration or Entity not found"),
    new ApiResponse(code = 409, message = "Method Configuration failed to resolve input expressions with the supplied Entity"),
    new ApiResponse(code = 500, message = "Rawls Internal Error")
  ))
  def submissionRoute = cookie("iPlanetDirectoryPro") { securityTokenCookie =>
    path("workspaces" / Segment / Segment / "submissions") { (workspaceNamespace, workspaceName) =>
      post {
        entity(as[SubmissionRequest]) { submission =>
          requestContext => perRequest(requestContext,
                                       WorkspaceService.props(workspaceServiceConstructor),
                                       WorkspaceService.CreateSubmission(WorkspaceName(workspaceNamespace,workspaceName),submission,securityTokenCookie))
        }
      }
    }
  }
}