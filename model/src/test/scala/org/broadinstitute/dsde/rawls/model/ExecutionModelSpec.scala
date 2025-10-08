package org.broadinstitute.dsde.rawls.model

import org.joda.time.DateTime
import org.scalatest.Assertions
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport._
import spray.json._

class ExecutionModelSpec extends AnyFlatSpec with Assertions with Matchers {

  behavior of "SubmissionRequest Deserialization"
  it should "deserialize perWorkflowCostCap correctly" in {
    val requestString =
      """{
        | "methodConfigurationNamespace": "testNamespace",
        | "methodConfigurationName": "testName",
        | "useCallCache": false,
        | "deleteIntermediateOutputFiles": false,
        | "perWorkflowCostCap": 23456789.01
        |}""".stripMargin

    val requestObj = SubmissionRequestFormat.read(requestString.parseJson)
    requestObj.perWorkflowCostCap should be(Some(BigDecimal("23456789.01")))
  }

  behavior of "SubmissionListResponse Serialization"

  it should "not include perWorkflowCostCap in json if specified as None" in {
    val responseObj = new SubmissionListResponse(
      "id",
      DateTime.now(),
      "testSubmitter",
      "testNamespace",
      "testName",
      false,
      None,
      SubmissionStatuses.Submitted,
      Map.empty,
      false,
      "testRoot",
      false,
      None,
      None
    )
    val serializedObj = SubmissionListResponseFormat.write(responseObj).toString()
    serializedObj should include("submissionId")
    serializedObj should include("submitter")
    serializedObj should include("testSubmitter")
    serializedObj should not(include("perWorkflowCostCap"))

  }

  it should "be able to write out the perWorkflowCostCap to json" in {
    val bigDecimalString = "23456789.01"
    val responseObj = new SubmissionListResponse(
      "id",
      DateTime.now(),
      "testSubmitter",
      "testNamespace",
      "testName",
      false,
      None,
      SubmissionStatuses.Submitted,
      Map.empty,
      false,
      "testRoot",
      false,
      None,
      None,
      perWorkflowCostCap = Some(BigDecimal(bigDecimalString))
    )
    val serializedObj = SubmissionListResponseFormat.write(responseObj).toString()

    serializedObj should include("perWorkflowCostCap")
    serializedObj should include(bigDecimalString)
  }

  behavior of "ExecutionServiceOutputs deserialization"

  it should "deserialize not-yet-archived outputs responses" in {
    val jsonString =
      """{
        | "id": "00112233-4455-6677-8899-aabbccddeeff",
        | "outputs": {
        |   "echo_strings.echo_files.out": "Hello World"
        | }
        |}""".stripMargin

    val jsonObj = ExecutionServiceOutputsFormat.read(jsonString.parseJson)
    jsonObj shouldBe ExecutionServiceOutputs(
      id = "00112233-4455-6677-8899-aabbccddeeff",
      outputs = Option(
        Map(
          "echo_strings.echo_files.out" -> Left(AttributeString("Hello World"))
        )
      ),
      message = None,
      metadataArchiveStatus = None
    )
  }

  it should "deserialize archived outputs responses" in {
    val jsonString =
      """{
        | "id": "00112233-4455-6677-8899-aabbccddeeff",
        | "message": "Cromwell has archived this workflow's metadata according to the lifecycle policy. The workflow completed at 2025-02-28T14:46:51.011Z, which was 17385233716 milliseconds ago. It is available in the archive bucket, or via a support request in the case of a managed instance.",
        | "metadataArchiveStatus": "ArchivedAndDeleted"
        |}""".stripMargin

    val jsonObj = ExecutionServiceOutputsFormat.read(jsonString.parseJson)
    jsonObj shouldBe ExecutionServiceOutputs(
      id = "00112233-4455-6677-8899-aabbccddeeff",
      outputs = None,
      message = Option(
        "Cromwell has archived this workflow's metadata according to the lifecycle policy. The workflow completed at 2025-02-28T14:46:51.011Z, which was 17385233716 milliseconds ago. It is available in the archive bucket, or via a support request in the case of a managed instance."
      ),
      metadataArchiveStatus = Option("ArchivedAndDeleted")
    )
  }

}
