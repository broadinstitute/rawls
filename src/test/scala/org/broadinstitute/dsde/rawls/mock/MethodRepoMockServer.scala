package org.broadinstitute.dsde.rawls.mock

import java.io.File

import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.model.AgoraEntity
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.mockserver.integration.ClientAndServer._
import org.mockserver.model.Header
import org.mockserver.model.HttpRequest.request
import org.mockserver.model.HttpResponse._
import spray.http.StatusCodes
import spray.json._

object MethodRepoMockServer {
  val port = 8987
  val mockServerBaseUrl = "http://localhost:" + port

  val jsonHeader = new Header("Content-Type", "application/json")
  val mockServer = startClientAndServer(port)

  def startServer = {

    // copy method config endpoint

    val copyMethodConfigPath = "/configurations"

    val goodResult = AgoraEntity(Some("workspace_test"), Some("rawls_test_good"), Some(1), None, None, None, None,
      Some("{\n  \"name\": \"rawls_test_1\",\n  \"rootEntityType\": \"rawls_test_type_1\",\n  \"methodNamespace\": \"rawls_test_m_namespace_1\",\n  \"methodName\": \"rawls_test_m_name_1\",\n  \"methodVersion\": \"rawls_test_m_version_1\",\n  \"prerequisites\": { },\n  \"inputs\": { },\n  \"outputs\": { },\n  \"workspaceName\": {\n    \"namespace\": \"rawls_test_w_namespace_1\",\n    \"name\": \"rawls_test_w_name_1\"\n  },\n  \"namespace\": \"rawls_test_namespace_1\"\n}"),
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
  }

  def stopServer = mockServer.stop()
}
