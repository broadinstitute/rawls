package org.broadinstitute.dsde.rawls.monitor

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.broadinstitute.dsde.rawls.google.GooglePubSubDAO.PubSubMessage
import org.broadinstitute.dsde.rawls.model.{RawlsUserEmail, WorkspaceName}

import java.util.UUID

class AvroUpsertMonitorMessageParserSpec extends AnyFlatSpec with Matchers {

  behavior of "AvroUpsertMonitorMessageParser"

  /* parsing works when all keys are supplied */
  it should "parse a valid message" in {
    val jobId = UUID.randomUUID()
    val attributes = Map(
      "workspaceNamespace" -> "my workspaceNamespace",
      "workspaceName" -> "my workspaceName",
      "userEmail" -> "my userEmail",
      "jobId" -> jobId.toString,
      "upsertFile" -> "my upsertFile",
      "isUpsert" -> "true"
    )
    val input = buildMessage(attributes)
    val parser = new AvroUpsertMonitorMessageParser(input)

    val expected = AvroUpsertAttributes(
      workspace = WorkspaceName("my workspaceNamespace", "my workspaceName"),
      userEmail = RawlsUserEmail("my userEmail"),
      importId = jobId,
      upsertFile = "my upsertFile",
      isUpsert = true
    )

    val actual = parser.parse

    actual shouldBe expected
  }

  /* when expected keys are missing, parsing fails  */
  it should "throw error when missing keys" in {
    val attributes = Map(
      "invalid" -> "doesn't matter"
    )
    val input = buildMessage(attributes)
    val parser = new AvroUpsertMonitorMessageParser(input)

    assertThrows[Exception] {
      parser.parse
    }
  }

  /* helper to create a PubSubMessage */
  private def buildMessage(attributes: Map[String, String]): PubSubMessage = {
    val ackId = UUID.randomUUID().toString
    val contents = "message content"
    PubSubMessage(ackId, contents, attributes)
  }

}
