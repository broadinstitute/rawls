package org.broadinstitute.dsde.rawls.model

import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.scalatest.{FlatSpec, Matchers}
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport._
import spray.json._

/**
  * Created by rtitle on 5/7/17.
  */
class ExecutionModelSpec extends FlatSpec with Matchers with RawlsTestUtils {

  "WorkflowQueueStatusByUserResponse" should "serialize/deserialize to/from JSON" in {
    val testResponse = WorkflowQueueStatusByUserResponse(
      statuses = Map(
        SubmissionStatuses.Accepted.toString -> 1,
        SubmissionStatuses.Evaluating.toString -> 5,
        SubmissionStatuses.Aborting.toString -> 100),
      users = Map(
        "user1" -> Map(
          SubmissionStatuses.Accepted.toString -> 1),
        "user2" -> Map(
          SubmissionStatuses.Evaluating.toString -> 2,
          SubmissionStatuses.Aborting.toString -> 1),
        "user3" -> Map(
          SubmissionStatuses.Evaluating.toString -> 3,
          SubmissionStatuses.Aborting.toString -> 99)),
      maxActiveWorkflowsTotal = 2000,
      maxActiveWorkflowsPerUser = 200)

    val expectedJson =
      """
        |{
        |  "statuses": {
        |    "Accepted": 1,
        |    "Evaluating": 5,
        |    "Aborting": 100
        |  },
        |  "users": {
        |    "user1": {
        |      "statuses": {
        |        "Accepted": 1
        |      }
        |    },
        |    "user2": {
        |      "statuses": {
        |        "Evaluating": 2,
        |        "Aborting": 1
        |      }
        |    },
        |    "user3": {
        |      "statuses": {
        |        "Evaluating": 3,
        |        "Aborting": 99
        |      }
        |    }
        |  },
        |  "maxActiveWorkflowsTotal": 2000,
        |  "maxActiveWorkflowsPerUser": 200
        |}
      """.stripMargin.parseJson

    // Verify round trip JSON serialization/deserialization
    val testJson = testResponse.toJson
    testJson should equal (expectedJson)
    testJson.convertTo[WorkflowQueueStatusByUserResponse] should equal (testResponse)
  }

}
