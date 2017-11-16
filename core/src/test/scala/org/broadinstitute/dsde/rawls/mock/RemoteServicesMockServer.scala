package org.broadinstitute.dsde.rawls.mock

import java.util.UUID
import java.util.concurrent.TimeUnit

import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.broadinstitute.dsde.rawls.model.{AgoraEntity, AgoraEntityType, AgoraStatus, ExecutionServiceStatus}
import org.broadinstitute.dsde.rawls.model.MethodRepoJsonSupport._
import org.mockserver.integration.ClientAndServer._
import org.mockserver.model.{Delay, Header, Parameter, ParameterBody}
import org.mockserver.model.HttpRequest.request
import org.mockserver.model.HttpResponse._
import spray.http.StatusCodes
import spray.json._
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.ExecutionServiceStatusFormat
import DefaultJsonProtocol._
import scala.concurrent.duration.FiniteDuration

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

class RemoteServicesMockServer(port:Int) extends RawlsTestUtils {
  val mockServerBaseUrl = "http://localhost:" + port

  val jsonHeader = new Header("Content-Type", "application/json")
  val mockServer = startClientAndServer(port)

  val defaultWorkflowSubmissionTimeout = FiniteDuration(1, TimeUnit.MINUTES)

  def startServer(numWorkflows: Int = 3) = {
    // copy method config endpoint

    val copyMethodConfigPath = "/configurations"

    val goodResult = AgoraEntity(Some("workspace_test"), Some("rawls_test_good"), Some(1), None, None, None, None,
      Some("{\"name\":\"testConfig1\",\"workspaceName\":{\"namespace\":\"myNamespace\",\"name\":\"myWorkspace\"},\"methodRepoMethod\":{\"methodNamespace\":\"ns-config\",\"methodName\":\"meth1\",\"methodVersion\":1},\"methodRepoConfig\":{\"methodConfigNamespace\":\"ns\",\"methodConfigName\":\"meth1\",\"methodConfigVersion\":1},\"outputs\":{\"p1\":\"prereq expr\"},\"inputs\":{\"o1\":\"output expr\"},\"rootEntityType\":\"Sample\",\"prerequisites\":{\"i1\":\"input expr\"},\"namespace\":\"ns\"}"),
      None, None)

    val emptyPayloadResult = AgoraEntity(Some("workspace_test"), Some("rawls_test_empty_payload"), Some(1), None, None, None, None,
      Some(""),
      None, None)

    val badPayloadResult = AgoraEntity(Some("workspace_test"), Some("rawls_test_bad_payload"), Some(1), None, None, None, None,
      Some("{\n  \"name\": \"invalid\",\n}"),
      None, None)

    val libraryResult = AgoraEntity(Some("workspace_test"), Some("rawls_test_library"), Some(1), None, None, None, None,
      Some("{\"name\":\"testConfig1\",\"workspaceName\":{\"namespace\":\"myNamespace\",\"name\":\"myWorkspace\"},\"methodRepoMethod\":{\"methodNamespace\":\"ns-config\",\"methodName\":\"meth1\",\"methodVersion\":1},\"methodRepoConfig\":{\"methodConfigNamespace\":\"ns\",\"methodConfigName\":\"meth1\",\"methodConfigVersion\":1},\"outputs\":{\"x1\":\"this.library:attr\"},\"inputs\":{\"o1\":\"output expr\"},\"rootEntityType\":\"Sample\",\"prerequisites\":{\"i1\":\"input expr\"},\"namespace\":\"ns\"}"),
      None, None)

    mockServer.when(
      request()
        .withMethod("POST")
        .withPath(copyMethodConfigPath)
    ).respond(
        response()
          .withHeaders(jsonHeader)
          .withBody(goodResult.toJson.prettyPrint)
          .withStatusCode(StatusCodes.OK.intValue)
      )

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

    mockServer.when(
      request()
        .withMethod("GET")
        .withPath(copyMethodConfigPath + "/workspace_test/rawls_test_library/1")
    ).respond(
      response()
        .withHeaders(jsonHeader)
        .withBody(libraryResult.toJson.prettyPrint)
        .withStatusCode(StatusCodes.OK.intValue)
    )

    val methodPath = "/methods"
    val threeStepWDL =
    """
      |task ps {
      |  command {
      |    ps
      |  }
      |  output {
      |    File procs = stdout()
      |  }
      |}
      |
      |task cgrep {
      |  File in_file
      |  String pattern
      |  command {
      |    grep '${pattern}' ${in_file} | wc -l
      |  }
      |  output {
      |    Int count = read_int(stdout())
      |  }
      |}
      |
      |task wc {
      |  File in_file
      |  command {
      |    cat ${in_file} | wc -l
      |  }
      |  output {
      |    Int count = read_int(stdout())
      |  }
      |}
      |
      |workflow three_step {
      |  call ps
      |  call cgrep {
      |    input: in_file=ps.procs
      |  }
      |  call wc {
      |    input: in_file=ps.procs
      |  }
      |}
    """.stripMargin

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
        .withPath(methodPath + "/ns-config/meth1/1")
    ).respond(
      response()
        .withHeaders(jsonHeader)
        .withBody(goodResult.toJson.prettyPrint)
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

    // Saving invalid WDL as a Method Repo Method is allowed

    val badSyntaxWDL = threeStepWDL.replace("workflow", "not-a-workflow")
    val badWDLMethod = AgoraEntity(Some("dsde"),Some("bad_wdl"),Some(1),None,None,None,None,Some(badSyntaxWDL),None,Some(AgoraEntityType.Workflow))

    mockServer.when(
      request()
        .withMethod("GET")
        .withPath(methodPath + "/dsde/bad_wdl/1")
    ).respond(
      response()
        .withHeaders(jsonHeader)
        .withBody(badWDLMethod.toJson.prettyPrint)
        .withStatusCode(StatusCodes.OK.intValue)
    )

    val noInputWdl =
      """
        |task t1 {
        |  command {
        |    echo "Hello"
        |  }
        |}
        |
        |workflow w1 {
        |  call t1
        |}
      """.stripMargin

    val noInputMethod = AgoraEntity(Some("dsde"), Some("no_input"), Some(1), None, None, None, None, Some(noInputWdl), None, Some(AgoraEntityType.Workflow))

    mockServer.when(
      request()
        .withMethod("GET")
        .withPath(methodPath + "/dsde/no_input/1")
    ).respond(
      response()
        .withHeaders(jsonHeader)
        .withBody(noInputMethod.toJson.prettyPrint)
        .withStatusCode(StatusCodes.OK.intValue)
    )

    val arrayWdl = """task aggregate_data {
                     |	Array[String] input_array
                     |
                     |	command {
                     |    echo "foo"
                     |
                     |	}
                     |
                     |	output {
                     |		Array[String] output_array = input_array
                     |	}
                     |
                     |	runtime {
                     |		docker : "broadinstitute/aaaa:31"
                     |	}
                     |
                     |	meta {
                     |		author : "Barack Obama"
                     |		email : "barryo@whitehouse.gov"
                     |	}
                     |
                     |}
                     |
                     |workflow aggregate_data_workflow {
                     |	call aggregate_data
                     |}""".stripMargin
    val arrayMethod = AgoraEntity(Some("dsde"),Some("array_task"),Some(1),None,None,None,None,Some(arrayWdl),None,Some(AgoraEntityType.Workflow))
    mockServer.when(
      request()
        .withMethod("GET")
        .withPath(methodPath + "/dsde/array_task/1")
    ).respond(
      response()
        .withHeaders(jsonHeader)
        .withBody(arrayMethod.toJson.prettyPrint)
        .withStatusCode(StatusCodes.OK.intValue)
    )

    mockServer.when(
      request()
        .withMethod("GET")
        .withPath("/status")
    ).respond(
      response()
        .withHeaders(jsonHeader)
        .withBody(AgoraStatus(true, Seq.empty).toJson.prettyPrint)
        .withStatusCode(StatusCodes.OK.intValue)
    )

    val submissionPath = "/api"
    mockServer.when(
      request()
        .withMethod("GET")
        .withPath(submissionPath + "/workflows/v1/.*/status")
    ).respond(
      response()
        .withHeaders(jsonHeader)
        .withBody(ExecutionServiceStatus("id", "Running").toJson.toString)
        .withStatusCode(StatusCodes.OK.intValue)
    )

    // delay for two seconds when the test asks for it
    // Don't support this when using a lot of workflows since the regex in mockServerContains
    // throws a StackOverflowError when called with too large a body.
    if (numWorkflows < 20) {
      mockServer.when(
        request()
          .withMethod("POST")
          .withPath(submissionPath + "/workflows/v1/batch")
          .withBody(mockServerContains("two_second_delay"))
      ).respond(
        response()
          .withHeaders(jsonHeader)
          .withBody(
            """[
              {"id": "69d1d92f-3895-4a7b-880a-82535e9a096e", "status": "Submitted"},
              {"id": "69d1d92f-3895-4a7b-880a-82535e9a096f", "status": "Submitted"},
              {"status": "error", "message": "stuff happens"}
              ]""")
          .withStatusCode(StatusCodes.Created.intValue)
          .withDelay(new Delay(TimeUnit.SECONDS, 2))
      )
    }

    mockServer.when(
      request()
        .withMethod("POST")
        .withPath(submissionPath + "/workflows/v1/batch")
    ).respond(
      response()
        .withHeaders(jsonHeader)
        .withBody {
          // Make the last one a failure
          (1 to numWorkflows).map(n => ExecutionServiceStatus(UUID.randomUUID().toString, if (n == numWorkflows) "Failed" else "Submitted")).toList.toJson.toString
        }
        .withStatusCode(StatusCodes.Created.intValue)
    )

    mockServer.when(
      request()
        .withMethod("POST")
        // this workflow exists
        .withPath(submissionPath + "/workflows/v1/69d1d92f-3895-4a7b-880a-82535e9a096e/abort")
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
        .withPath(submissionPath + "/workflows/v1/workflowA/abort")
    ).respond(
        response()
          .withHeaders(jsonHeader)
          .withBody(
            """{
    "id": "workflowA",
    "status": "Aborted"
}""")
          .withStatusCode(StatusCodes.OK.intValue)
      )

    mockServer.when(
      request()
        .withMethod("POST")
        .withPath(submissionPath + "/workflows/v1/workflowB/abort")
    ).respond(
        response()
          .withHeaders(jsonHeader)
          .withBody(
            """{
    "id": "workflowB",
    "status": "Aborted"
}""")
          .withStatusCode(StatusCodes.OK.intValue)
      )

    mockServer.when(
      request()
        .withMethod("POST")
        .withPath(submissionPath + "/workflows/v1/workflowC/abort")
    ).respond(
        response()
          .withHeaders(jsonHeader)
          .withBody(
            """{
    "id": "workflowC",
    "status": "Aborted"
}""")
          .withStatusCode(StatusCodes.OK.intValue)
      )

    mockServer.when(
      request()
        .withMethod("POST")
        .withPath(submissionPath + "/workflows/v1/workflowD/abort")
    ).respond(
        response()
          .withHeaders(jsonHeader)
          .withBody(
            """{
    "id": "workflowD",
    "status": "Aborted"
}""")
          .withStatusCode(StatusCodes.OK.intValue)
      )

    mockServer.when(
      request()
        .withMethod("POST")
         // already_terminal_workflow
        .withPath(submissionPath + "/workflows/v1/45def17d-40c2-44cc-89bf-9e77bc2c8778/abort")
    ).respond(
        response()
          .withHeaders(jsonHeader)
          .withStatusCode(StatusCodes.Forbidden.intValue)
      )

    mockServer.when(
      request()
        .withMethod("POST")
        .withPath(submissionPath + "/workflows/v1/45def17d-40c2-44cc-89bf-9e77bc2c9999/abort")
    ).respond(
        response()
          .withHeaders(jsonHeader)
          .withStatusCode(StatusCodes.BadRequest.intValue)
      )

    mockServer.when(
      request()
        .withMethod("POST")
        .withPath(submissionPath + "/workflows/v1/malformed_workflow/abort")
    ).respond(
        response()
          .withHeaders(jsonHeader)
          .withStatusCode(StatusCodes.BadRequest.intValue)
      )

    mockServer.when(
      request()
        .withMethod("GET")
        .withPath(submissionPath + "/workflows/v1/69d1d92f-3895-4a7b-880a-82535e9a096e/logs")
    ).respond(
        response()
          .withHeaders(jsonHeader)
          .withBody(
            """
              |{
              |  "id": "69d1d92f-3895-4a7b-880a-82535e9a096e",
              |  "calls": {
              |    "wf.x": [{
              |      "stdout": "gs://cromwell-dev/cromwell-executions/wf/this_workflow_exists/call-x/job.stdout.txt",
              |      "stderr": "gs://cromwell-dev/cromwell-executions/wf/this_workflow_exists/call-x/job.stderr.txt"
              |    }],
              |    "wf.y": [{
              |      "stdout": "gs://cromwell-dev/cromwell-executions/wf/this_workflow_exists/call-y/job.stdout-1.txt",
              |      "stderr": "gs://cromwell-dev/cromwell-executions/wf/this_workflow_exists/call-y/job.stderr-1.txt",
              |      "backendLogs": {
              |        "log": "gs://cromwell-dev/cromwell-executions/wf/this_workflow_exists/call-y/jes.log",
              |        "stdout": "gs://cromwell-dev/cromwell-executions/wf/this_workflow_exists/call-y/jes-stdout.log",
              |        "stderr": "gs://cromwell-dev/cromwell-executions/wf/this_workflow_exists/call-y/jes-stderr.log"
              |      }
              |    },
              |    {
              |      "stdout": "gs://cromwell-dev/cromwell-executions/wf/this_workflow_exists/call-y/job.stdout-2.txt",
              |      "stderr": "gs://cromwell-dev/cromwell-executions/wf/this_workflow_exists/call-y/job.stderr-2.txt"
              |    }]
              |  }
              |}
              """.stripMargin)
          .withStatusCode(StatusCodes.Created.intValue)
      )

    mockServer.when(
      request()
        .withMethod("GET")
        .withPath(submissionPath + "/workflows/v1/69d1d92f-3895-4a7b-880a-82535e9a096e/outputs")
    ).respond(
        response()
          .withHeaders(jsonHeader)
          .withBody(
            """
              |{
              |  "id": "this_workflow_exists",
              |  "outputs": {
              |    "wf.x.four": 4,
              |    "wf.x.five": 4,
              |    "wf.y.six": 4
              |  }
              |}
            """.stripMargin)
          .withStatusCode(StatusCodes.Created.intValue)
      )

    mockServer.when(
      request()
        .withMethod("GET")
        .withPath(submissionPath + "/workflows/v1/69d1d92f-3895-4a7b-880a-82535e9a096e/metadata")
    ).respond(
        response()
          .withHeaders(jsonHeader)
          .withBody(
            """
              |{
              |  "id": "69d1d92f-3895-4a7b-880a-82535e9a096e",
              |  "status": "Unknown",
              |  "submission": "2010-09-10T11:12:13.456Z",
              |  "outputs": {"test": ["baz", "bar", "foo"]},
              |  "calls": {}
              |}
            """.stripMargin)
          .withStatusCode(StatusCodes.Created.intValue)
      )

    mockServer.when(
      request()
        .withMethod("GET")
        .withPath(submissionPath + "/workflows/v1/8afafe21-2b70-4180-a565-748cb573e10c/outputs")
    ).respond(
        response()
          .withHeaders(jsonHeader)
          .withBody(
            """
              |{
              |  "id": "8afafe21-2b70-4180-a565-748cb573e10c",
              |  "outputs": {
              |    "aggregate_data_workflow.aggregate_data.output_array": [["foo", "bar"], ["baz", "qux"]]
              |  }
              |}
            """.stripMargin)
          .withStatusCode(StatusCodes.Created.intValue)
      )

    mockServer.when(
      request()
        .withMethod("GET")
        .withPath(submissionPath + "/workflows/v1/8afafe21-2b70-4180-a565-748cb573e10c/metadata")
    ).respond(
        response()
          .withHeaders(jsonHeader)
          .withBody(
            """
              |{
              |  "id": "8afafe21-2b70-4180-a565-748cb573e10c",
              |  "status": "Unknown",
              |  "submission": "2010-09-10T11:12:13.456Z",
              |  "outputs": {"aggregate_data_workflow.aggregate_data.output_array": [["foo", "bar"], ["baz", "qux"]]},
              |  "calls": {}
              |}
            """.stripMargin)
          .withStatusCode(StatusCodes.Created.intValue)
      )

    mockServer.when(
      request()
        .withMethod("GET")
        .withPath(submissionPath + "/workflows/v1/8afafe21-2b70-4180-a565-748cb573e10c/logs")
    ).respond(
        response()
          .withHeaders(jsonHeader)
          .withBody(
            """
              |{
              |  "id": "8afafe21-2b70-4180-a565-748cb573e10c",
              |  "calls": {
              |  }
              |}
            """.stripMargin)
          .withStatusCode(StatusCodes.Created.intValue)
      )

    mockServer.when(
      request()
        .withMethod("GET")
        .withPath(submissionPath + "/workflows/v1/29b2e816-ecaf-11e6-b006-92361f002671/outputs")
    ).respond(
      response()
        .withHeaders(jsonHeader)
        .withStatusCode(StatusCodes.BadRequest.intValue)
    )

    mockServer.when(
      request()
        .withMethod("GET")
        .withPath(submissionPath + "/workflows/v1/29b2e816-ecaf-11e6-b006-92361f002671/metadata")
    ).respond(
      response()
        .withHeaders(jsonHeader)
        .withStatusCode(StatusCodes.BadRequest.intValue)
    )

    mockServer.when(
      request()
        .withMethod("GET")
        .withPath("/engine/v1/version")
    ).respond(
      response()
        .withHeaders(jsonHeader)
        .withStatusCode(StatusCodes.OK.intValue)
        .withBody("""{"cromwell":"25"}""")
    )
  }

  def stopServer = mockServer.stop()

  def reset = mockServer.reset()
}
