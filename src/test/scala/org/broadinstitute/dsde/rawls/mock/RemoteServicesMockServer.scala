package org.broadinstitute.dsde.rawls.mock

import java.io.File

import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.model.{AgoraEntity,AgoraEntityType}
import org.broadinstitute.dsde.rawls.model.MethodRepoJsonSupport._
import org.mockserver.integration.ClientAndServer._
import org.mockserver.model.Header
import org.mockserver.model.HttpRequest.request
import org.mockserver.model.HttpResponse._
import spray.http.StatusCodes
import spray.json._

/**
 * Mock server interface for the methods repo and execution service.
 */
object RemoteServicesMockServer {
  var currentPort = 30000
  def apply() = {
    currentPort += 1
    new RemoteServicesMockServer(currentPort)
  }
}

class RemoteServicesMockServer(port:Int) {
  val mockServerBaseUrl = "http://localhost:" + port

  val jsonHeader = new Header("Content-Type", "application/json")
  val mockServer = startClientAndServer(port)

  def startServer = {

    // copy method config endpoint

    val copyMethodConfigPath = "/configurations"

    val goodResult = AgoraEntity(Some("workspace_test"), Some("rawls_test_good"), Some(1), None, None, None, None,
      Some("{\"name\":\"testConfig1\",\"workspaceName\":{\"namespace\":\"myNamespace\",\"name\":\"myWorkspace\"},\"methodRepoMethod\":{\"methodNamespace\":\"ns-config\",\"methodName\":\"meth1\",\"methodVersion\":\"1\"},\"methodRepoConfig\":{\"methodConfigNamespace\":\"ns\",\"methodConfigName\":\"meth1\",\"methodConfigVersion\":\"1\"},\"outputs\":{\"p1\":\"prereq expr\"},\"inputs\":{\"o1\":\"output expr\"},\"rootEntityType\":\"Sample\",\"prerequisites\":{\"i1\":\"input expr\"},\"namespace\":\"ns\"}"),
      None, None)

    val emptyPayloadResult = AgoraEntity(Some("workspace_test"), Some("rawls_test_empty_payload"), Some(1), None, None, None, None,
      Some(""),
      None, None)

    val badPayloadResult = AgoraEntity(Some("workspace_test"), Some("rawls_test_bad_payload"), Some(1), None, None, None, None,
      Some("{\n  \"name\": \"invalid\",\n}"),
      None, None)

    mockServer.when(
      request()
        .withMethod("GET")
        .withPath(copyMethodConfigPath + "/workspace_test/rawls_test_good/1")
    ).respond(
        response()
          .withHeaders(jsonHeader)
          .withBody(goodResult.toJson.prettyPrint)
          .withStatusCode(StatusCodes.OK.intValue)
      )

    mockServer.when(
      request()
        .withMethod("GET")
        .withPath(copyMethodConfigPath + "/workspace_test/rawls_test_missing/1")
    ).respond(
        response()
          .withHeaders(jsonHeader)
          .withStatusCode(StatusCodes.NotFound.intValue)
      )

    mockServer.when(
      request()
        .withMethod("GET")
        .withPath(copyMethodConfigPath + "/workspace_test/rawls_test_empty_payload/1")
    ).respond(
        response()
          .withHeaders(jsonHeader)
          .withBody(emptyPayloadResult.toJson.prettyPrint)
          .withStatusCode(StatusCodes.OK.intValue)
      )

    mockServer.when(
      request()
        .withMethod("GET")
        .withPath(copyMethodConfigPath + "/workspace_test/rawls_test_bad_payload/1")
    ).respond(
        response()
          .withHeaders(jsonHeader)
          .withBody(badPayloadResult.toJson.prettyPrint)
          .withStatusCode(StatusCodes.OK.intValue)
      )

    val methodPath = "/methods"
    val threeStepWDL =
      """task ps {
        command {
          ps
        }
        output {
          File procs = stdout()
        }
      }
      task cgrep {
        command {
          grep '${pattern}' ${File in_file} | wc -l
        }
        output {
          Int count = read_int(stdout())
        }
      }
      task wc {
        command {
          cat ${File in_file} | wc -l
        }
        output {
          Int count = read_int(stdout())
        }
      }
      workflow three_step {
        call ps
        call cgrep {
          input: in_file=ps.procs
        }
        call wc {
          input: in_file=ps.procs
        }
      }
      """
    val threeStepMethod = AgoraEntity(Some("dsde"),Some("three_step"),Some(1),None,None,None,None,Some(threeStepWDL),None,Some(AgoraEntityType.Workflow))
    mockServer.when(
      request()
        .withMethod("GET")
        .withPath(methodPath + "/dsde/three_step/1")
    ).respond(
        response()
          .withHeaders(jsonHeader)
          .withBody(threeStepMethod.toJson.prettyPrint)
          .withStatusCode(StatusCodes.OK.intValue)
      )

    mockServer.when(
      request()
        .withMethod("GET")
        .withPath(methodPath + "/dsde/three_step/2")
    ).respond(
        response()
          .withStatusCode(StatusCodes.NotFound.intValue)
      )

    val singleInputWdl =
      """
        |task t1 {
        |  command {
        |    echo ${Int int_arg}
        |  }
        |}
        |
        |workflow w1 {
        |  call t1
        |}
      """.stripMargin

    val singleInputMethod = AgoraEntity(Some("dsde"), Some("single_input"), Some(1), None, None, None, None,
      Some(singleInputWdl), None, Some(AgoraEntityType.Workflow))

    mockServer.when(
      request()
        .withMethod("GET")
        .withPath(methodPath + "/dsde/single_input/1")
    ).respond(
        response()
          .withHeaders(jsonHeader)
          .withBody(threeStepMethod.toJson.prettyPrint)
          .withStatusCode(StatusCodes.OK.intValue)
      )

    val submissionPath = "/workflows/v1"
    mockServer.when(
      request()
        .withMethod("POST")
        .withPath(submissionPath)
    ).respond(
        response()
          .withHeaders(jsonHeader)
          .withBody(
            """{
    "id": "69d1d92f-3895-4a7b-880a-82535e9a096e",
    "status": "Submitted"
}""")
          .withStatusCode(StatusCodes.Created.intValue)
      )

    mockServer.when(
      request()
        .withMethod("POST")
        // this workflow exists
        .withPath("/workflows/v1/69d1d92f-3895-4a7b-880a-82535e9a096e/abort")
    ).respond(
        response()
          .withHeaders(jsonHeader)
          .withBody(
            """{
    "id": "69d1d92f-3895-4a7b-880a-82535e9a096e",
    "status": "Aborted"
}""")
          .withStatusCode(StatusCodes.OK.intValue)
      )

    mockServer.when(
      request()
        .withMethod("POST")
         // already_terminal_workflow
        .withPath("/workflows/v1/45def17d-40c2-44cc-89bf-9e77bc2c8778/abort")
    ).respond(
        response()
          .withHeaders(jsonHeader)
          .withStatusCode(StatusCodes.Forbidden.intValue)
      )

    mockServer.when(
      request()
        .withMethod("POST")
        .withPath("/workflows/v1/45def17d-40c2-44cc-89bf-9e77bc2c9999/abort")
    ).respond(
        response()
          .withHeaders(jsonHeader)
          .withStatusCode(StatusCodes.BadRequest.intValue)
      )

    mockServer.when(
      request()
        .withMethod("POST")
        .withPath("/workflows/v1/malformed_workflow/abort")
    ).respond(
        response()
          .withHeaders(jsonHeader)
          .withStatusCode(StatusCodes.BadRequest.intValue)
      )
  }

  def stopServer = mockServer.stop()
}
