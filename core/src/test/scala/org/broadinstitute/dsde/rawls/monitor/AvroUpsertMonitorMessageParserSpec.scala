package org.broadinstitute.dsde.rawls.monitor

import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.google.GooglePubSubDAO.PubSubMessage
import org.broadinstitute.dsde.rawls.model.{RawlsUserEmail, WorkspaceName}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class AvroUpsertMonitorMessageParserSpec
    extends TestDriverComponent
    with AnyFlatSpecLike
    with Matchers
    with ScalaFutures {

  behavior of "AvroUpsertMonitorMessageParser"

  /* when expected keys are missing, parsing fails  */
  it should "throw error when missing keys" in {
    val attributes = Map(
      "invalid" -> "doesn't matter"
    )
    val parser = parserFor(attributes)

    assertThrows[Exception] {
      parser.parse.futureValue
    }
  }

  /* parsing works when all keys are supplied */
  it should "parse a valid message from Import Service" in {
    val jobId = UUID.randomUUID()
    // note the "isCWDS" key is missing
    val attributes = Map(
      "workspaceNamespace" -> "my workspaceNamespace",
      "workspaceName" -> "my workspaceName",
      "userEmail" -> "my userEmail",
      "jobId" -> jobId.toString,
      "upsertFile" -> "my upsertFile",
      "isUpsert" -> "true"
    )
    val parser = parserFor(attributes)

    val expected = AvroUpsertAttributes(
      workspace = WorkspaceName("my workspaceNamespace", "my workspaceName"),
      userEmail = RawlsUserEmail("my userEmail"),
      importId = jobId,
      upsertFile = "my upsertFile",
      isUpsert = true,
      isCwds = false
    )

    parser.parse.futureValue shouldBe expected
  }

  it should "parse a valid message from cWDS and retrieve the workspace namespace/name" in withConstantTestDatabase {
    val jobId = UUID.randomUUID()
    val attributes = Map(
      "workspaceId" -> constantData.workspace.workspaceId,
      "userEmail" -> "my userEmail",
      "jobId" -> jobId.toString,
      "upsertFile" -> "my upsertFile",
      "isUpsert" -> "true",
      "isCWDS" -> "true"
    )
    val parser = parserFor(attributes)

    val expected = AvroUpsertAttributes(
      workspace = constantData.workspace.toWorkspaceName,
      userEmail = RawlsUserEmail("my userEmail"),
      importId = jobId,
      upsertFile = "my upsertFile",
      isUpsert = true,
      isCwds = true
    )

    parser.parse.futureValue shouldBe expected
  }

  /* helper to create a AvroUpsertMonitorMessageParser for a PubSubMessage with the given attributes */
  private def parserFor(attributes: Map[String, String]): AvroUpsertMonitorMessageParser = {
    val ackId = UUID.randomUUID().toString
    val contents = "message content"
    val input = PubSubMessage(ackId, contents, attributes)

    new AvroUpsertMonitorMessageParser(input, slickDataSource)
  }

}
