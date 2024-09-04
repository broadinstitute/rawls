package org.broadinstitute.dsde.rawls.model

import org.joda.time.DateTime
import org.scalatest.Assertions
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport._
import spray.json._

class ExecutionModelSpec extends AnyFlatSpec with Assertions with Matchers {

  behavior of "SubmissionRequest Deserialization"
  it should "deserialize costCapThreshold correctly" in {
    val requestString =
      """{
        | "methodConfigurationNamespace": "testNamespace",
        | "methodConfigurationName": "testName",
        | "useCallCache": false,
        | "deleteIntermediateOutputFiles": false,
        | "costCapThreshold": 23456789.01
        |}""".stripMargin

    val requestObj = SubmissionRequestFormat.read(requestString.parseJson)
    requestObj.costCapThreshold should be(Some(BigDecimal("23456789.01")))
  }

  behavior of "SubmissionListResponse Serialization"

  it should "not include costCapThreshold in json if specified as None" in {
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
    serializedObj should not(include("costCapThreshold"))

  }

  it should "be able to write out the costCapThreshold to json" in {
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
      costCapThreshold = Some(BigDecimal(bigDecimalString))
    )
    val serializedObj = SubmissionListResponseFormat.write(responseObj).toString()

    serializedObj should include("costCapThreshold")
    serializedObj should include(bigDecimalString)
  }

}
