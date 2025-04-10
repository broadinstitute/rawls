package org.broadinstitute.dsde.rawls.metrics

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.dsde.rawls.dataaccess.slick.{SubmissionRecord, TestDriverComponent}
import org.broadinstitute.dsde.rawls.metrics.logEvents.SubmissionEvent
import org.broadinstitute.dsde.rawls.model.{
  MethodConfiguration,
  MethodRepoMethod,
  RawlsUserEmail,
  RawlsUserSubjectId,
  UserInfo
}
import org.mockserver.integration.ClientAndServer
import org.mockserver.model.{HttpRequest, HttpResponse, JsonBody, MediaType}
import org.mockserver.verify.VerificationTimes
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID

class BardServiceSpec extends AnyFlatSpec with TestDriverComponent with Matchers {

  val workspaceId = UUID.randomUUID()
  val submissionRec = SubmissionRecord(
    UUID.randomUUID(),
    workspaceId,
    Timestamp.from(Instant.now()),
    "test@testemail.test",
    12345L,
    Some(67890L),
    "Submitted",
    false,
    false,
    false,
    0L,
    None,
    Some("24680"),
    Some("13579"),
    Some("Comment"),
    "root",
    false,
    None,
    None,
    None,
    None
  )
  val methodRepoMethod = MethodRepoMethod("methodNamespace", "methodName", 1)
  val methodConfig = MethodConfiguration("namespace",
                                         "name",
                                         submissionRec.rootEntityType,
                                         None,
                                         Map.empty,
                                         Map.empty,
                                         methodRepoMethod,
                                         1,
                                         false,
                                         None,
                                         None
  )
  val workflowIds = Seq(1L, 2L)
  val externalIds = Seq(UUID.randomUUID().toString, UUID.randomUUID().toString)
  val user =
    UserInfo(RawlsUserEmail("test@test.com"), OAuth2BearerToken("token"), 5000L, RawlsUserSubjectId("1234567890"), None)

  val submissionEvent = SubmissionEvent(
    submissionRec.id.toString,
    workspaceId.toString,
    methodConfig.toId,
    methodConfig.namespace,
    methodConfig.name,
    methodConfig.methodRepoMethod.methodUri,
    methodConfig.methodRepoMethod.repo.scheme,
    methodConfig.methodConfigVersion,
    methodConfig.dataReferenceName.map(_.toString()),
    workflowIds,
    externalIds,
    submissionRec.rootEntityType,
    submissionRec.useCallCache,
    submissionRec.useReferenceDisks,
    submissionRec.memoryRetryMultiplier,
    submissionRec.ignoreEmptyOutputs,
    submissionRec.userComment
  )

  behavior of "BardService"

  it should "connect to the Bard instance via client" in {
    val mockServer = ClientAndServer.startClientAndServer()
    try {
      val bardApiPath = "/api/eventLog/v1/rawls/submission:summary"
      val bardUrl = "http://localhost:" + mockServer.getPort
      //  Mock the server response
      mockServer
        .when(HttpRequest.request(bardApiPath).withMethod("POST"))
        .respond(HttpResponse.response.withStatusCode(200).withContentType(MediaType.APPLICATION_JSON))

      val bardService = new BardService(true, bardUrl, 10)

      bardService.sendEvent(submissionEvent, user)

      mockServer.verify(
        HttpRequest
          .request(bardApiPath)
          .withMethod("POST")
          .withBody(new JsonBody(s"""{
                                    |   "properties": {
                                    |        "submissionId": "${submissionRec.id.toString}",
                                    |        "workspaceId": "${workspaceId.toString}",
                                    |        "methodId": "${methodConfig.toId}",
                                    |        "methodNamespace": "${methodConfig.namespace}",
                                    |        "methodName": "${methodConfig.name}",
                                    |        "methodUri": "${methodConfig.methodRepoMethod.methodUri}",
                                    |        "methodRepository": "${methodConfig.methodRepoMethod.repo.scheme}",
                                    |        "methodConfigVersion": ${methodConfig.methodConfigVersion},
                                    |        "methodDataReferenceName": ${methodConfig.dataReferenceName
                                     .map(_.toString())
                                     .orNull},
                                    |        "rawlsWorkflowIds": [${workflowIds.mkString(",")}],
                                    |        "externalIds": ["${externalIds.mkString("\",\"")}"],
                                    |        "rootEntityType": "${submissionRec.rootEntityType.get}",
                                    |        "useCallCaching": ${submissionRec.useCallCache},
                                    |        "useReferenceDisks": ${submissionRec.useReferenceDisks},
                                    |        "memoryRetryMultiplier": ${submissionRec.memoryRetryMultiplier},
                                    |        "ignoreEmtpyOutputs": ${submissionRec.ignoreEmptyOutputs},
                                    |        "userComment": "${submissionRec.userComment.get}",
                                    |        "pushToMixpanel": false,
                                    |        "event" : "submission:summary"
                                    |   }
                                    | }
                                    |""".stripMargin)),
        VerificationTimes.exactly(1)
      )
    } finally if (mockServer != null) mockServer.close()
  }

  it should "not send an event if Bard is disabled" in {
    val mockServer = ClientAndServer.startClientAndServer()
    try {
      val bardApiPath = "/api/eventLog/v1/rawls/submission:summary"
      val bardUrl = "http://localhost:" + mockServer.getPort
      //  Mock the server response
      mockServer
        .when(HttpRequest.request(bardApiPath).withMethod("POST"))
        .respond(HttpResponse.response.withStatusCode(200).withContentType(MediaType.APPLICATION_JSON))

      val bardService = new BardService(false, bardUrl, 10)

      bardService.sendEvent(submissionEvent, user)

      mockServer.verify(
        HttpRequest
          .request(bardApiPath)
          .withMethod("POST"),
        VerificationTimes.exactly(0)
      )
    } finally if (mockServer != null) mockServer.close()
  }
}
