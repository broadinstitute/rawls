package org.broadinstitute.dsde.rawls.webservice

import java.util.logging.Logger
import javax.ws.rs.Path
import com.wordnik.swagger.annotations._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import spray.httpx.SprayJsonSupport._
import spray.routing._

/**
 * Created by dvoet on 6/4/15.
 */
@Api(value = "/workspaces/{workspaceNamespace}/{workspaceName}/jobs", description = "Job API")
trait JobApiService extends HttpService with PerRequestCreator {

  val workspaceServiceConstructor: () => WorkspaceService
  val jobRoutes = submitJobRoute

  @ApiOperation(value = "Submit a job.",
    nickname = "jobSubmit",
    httpMethod = "POST",
    produces = "application/json",
    response = classOf[JobStatus])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "jobDescription", required = true, dataType = "org.broadinstitute.dsde.rawls.model.JobDescription", paramType = "body", value = "Description of job to submit.")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 201, message = "Successful Request"),
    new ApiResponse(code = 404, message = "Method Configuration or Entity not found"),
    new ApiResponse(code = 409, message = "Method Configuration failed to resolve input expressions with the supplied Entity"),
    new ApiResponse(code = 500, message = "Rawls Internal Error")
  ))
  def submitJobRoute = cookie("iPlanetDirectoryPro") { securityTokenCookie =>
    path("workspaces" / Segment / Segment / "jobs") { (workspaceNamespace, workspaceName) =>
      post {
        entity(as[JobDescription]) { jobDesc =>
          requestContext => perRequest(requestContext,
                                       WorkspaceService.props(workspaceServiceConstructor),
                                       WorkspaceService.SubmitJob(WorkspaceName(workspaceNamespace,workspaceName),jobDesc,securityTokenCookie))
        }
      }
    }
  }
}