package org.broadinstitute.dsde.rawls.model

import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import spray.json._

/**
  * Created by rtitle on 5/7/17.
  */
class ExecutionModelSpec extends AnyFlatSpec with Matchers {

  "SubmissionRequest deserialization" should "translate null to None" in {
    import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.SubmissionRequestFormat

    // We know `null` is possible for `entityType` and `entityName` because this JSON is derived
    // from what the FC UI actually generated and came through in the user report [WA-135]
    val inputJSON = """{
                      |"methodConfigurationNamespace": "asdf",
                      |"methodConfigurationName": "echo",
                      |"entityType": null,
                      |"entityName": null,
                      |"useCallCache": true,
                      |"expression": null,
                      |"workflowFailureMode": null,
                      |"deleteIntermediateOutputFiles": false,
                      |"useReferenceDisks": true,
                      |"memoryRetryMultiplier": 3.141,
                      |"ignoreEmptyOutputs": false
                      |}""".stripMargin.parseJson.asJsObject

    SubmissionRequestFormat.read(inputJSON) shouldEqual {
      SubmissionRequest(
        methodConfigurationNamespace = "asdf",
        methodConfigurationName = "echo",
        useCallCache = true,
        entityType = None,
        entityName = None,
        expression = None,
        workflowFailureMode = None,
        deleteIntermediateOutputFiles = false,
        useReferenceDisks = true,
        memoryRetryMultiplier = 3.141,
        ignoreEmptyOutputs = false
      )
    }
  }

  "SubmissionRequest deserialization" should "translate missing fields to None" in {
    import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.SubmissionRequestFormat

    val inputJSON = """{
                      |"methodConfigurationNamespace": "asdf",
                      |"methodConfigurationName": "echo",
                      |"useCallCache": true
                      |}""".stripMargin.parseJson.asJsObject

    SubmissionRequestFormat.read(inputJSON) shouldEqual {
      SubmissionRequest(
        methodConfigurationNamespace = "asdf",
        methodConfigurationName = "echo",
        useCallCache = true,
        entityType = None,
        entityName = None,
        expression = None,
        workflowFailureMode = None,
        deleteIntermediateOutputFiles = false,
        ignoreEmptyOutputs = false
      )
    }
  }

  "WorkflowQueueStatusByUserResponse" should "serialize/deserialize to/from JSON" in {
    val testResponse = WorkflowQueueStatusByUserResponse(
      statuses = Map(SubmissionStatuses.Accepted.toString -> 1,
                     SubmissionStatuses.Evaluating.toString -> 5,
                     SubmissionStatuses.Aborting.toString -> 100
      ),
      users = Map(
        "user1" -> Map(SubmissionStatuses.Accepted.toString -> 1),
        "user2" -> Map(SubmissionStatuses.Evaluating.toString -> 2, SubmissionStatuses.Aborting.toString -> 1),
        "user3" -> Map(SubmissionStatuses.Evaluating.toString -> 3, SubmissionStatuses.Aborting.toString -> 99)
      ),
      maxActiveWorkflowsTotal = 2000,
      maxActiveWorkflowsPerUser = 200
    )

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
    testJson should equal(expectedJson)
    testJson.convertTo[WorkflowQueueStatusByUserResponse] should equal(testResponse)
  }

  "ExecutionServiceWorkflowOptions" should "serialize/deserialize to/from JSON" in {
    val test = ExecutionServiceWorkflowOptions(
      jes_gcs_root = "jes_gcs_root",
      final_workflow_outputs_dir = None,
      final_workflow_outputs_mode = None,
      google_project = "google_project",
      account_name = "account_name",
      google_compute_service_account = "account@foo.com",
      user_service_account_json =
        """{
          |  "type": "service_account",
          |  "project_id": "broad-dsde-dev",
          |  "proovate_key_id": "120924d141277cef7a976320d3dc3e4e298ac447",
          |  "proovate_key": "-----BEGIN proovate KEY-----\naloha\n-----END proovate KEY-----\n",
          |  "client_email": "pet-110347448408766049948@broad-dsde-dev.iam.gserviceaccount.com",
          |  "client_id": "110086970853956779852",
          |  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
          |  "token_uri": "https://accounts.google.com/o/oauth2/token",
          |  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
          |  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/pet-110347448408766049948%40broad-dsde-dev.iam.gserviceaccount.com"
          |}""".stripMargin,
      final_workflow_log_dir = "final_workflow_log_dir",
      default_runtime_attributes = None,
      read_from_cache = true,
      delete_intermediate_output_files = true,
      use_reference_disks = true,
      memory_retry_multiplier = 2.718,
      backend = CromwellBackend("PAPIv2"),
      workflow_failure_mode = Some(WorkflowFailureModes.ContinueWhilePossible),
      ignore_empty_outputs = false,
      monitoring_script = Option("script"),
      monitoring_image = Option("image"),
      monitoring_image_script = Option("image script")
    )

    val expectedJson =
      """
        |{
        |  "jes_gcs_root": "jes_gcs_root",
        |  "google_project": "google_project",
        |  "account_name": "account_name",
        |  "google_compute_service_account": "account@foo.com",
        |  "google_labels": {},
        |  "user_service_account_json": "{\n  \"type\": \"service_account\",\n  \"project_id\": \"broad-dsde-dev\",\n  \"proovate_key_id\": \"120924d141277cef7a976320d3dc3e4e298ac447\",\n  \"proovate_key\": \"-----BEGIN proovate KEY-----\\naloha\\n-----END proovate KEY-----\\n\",\n  \"client_email\": \"pet-110347448408766049948@broad-dsde-dev.iam.gserviceaccount.com\",\n  \"client_id\": \"110086970853956779852\",\n  \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",\n  \"token_uri\": \"https://accounts.google.com/o/oauth2/token\",\n  \"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\",\n  \"client_x509_cert_url\": \"https://www.googleapis.com/robot/v1/metadata/x509/pet-110347448408766049948%40broad-dsde-dev.iam.gserviceaccount.com\"\n}",
        |  "final_workflow_log_dir": "final_workflow_log_dir",
        |  "read_from_cache": true,
        |  "delete_intermediate_output_files": true,
        |  "use_reference_disks": true,
        |  "memory_retry_multiplier": 2.718,
        |  "backend": "PAPIv2",
        |  "workflow_failure_mode": "ContinueWhilePossible",
        |  "ignore_empty_outputs": false,
        |  "monitoring_script": "script",
        |  "monitoring_image": "image",
        |  "monitoring_image_script": "image script"
        |}
      """.stripMargin.parseJson

    // Verify round trip JSON serialization/deserialization
    val testJson = test.toJson
    testJson should equal(expectedJson)
    testJson.convertTo[ExecutionServiceWorkflowOptions] should equal(test)

    // Verify it works with no workflow_failure_mode
    val noFailureMode = test.copy(workflow_failure_mode = None)
    val expectedJsonNoFailureMode =
      """
        |{
        |  "jes_gcs_root": "jes_gcs_root",
        |  "google_project": "google_project",
        |  "account_name": "account_name",
        |  "google_compute_service_account": "account@foo.com",
        |  "google_labels": {},
        |  "user_service_account_json": "{\n  \"type\": \"service_account\",\n  \"project_id\": \"broad-dsde-dev\",\n  \"proovate_key_id\": \"120924d141277cef7a976320d3dc3e4e298ac447\",\n  \"proovate_key\": \"-----BEGIN proovate KEY-----\\naloha\\n-----END proovate KEY-----\\n\",\n  \"client_email\": \"pet-110347448408766049948@broad-dsde-dev.iam.gserviceaccount.com\",\n  \"client_id\": \"110086970853956779852\",\n  \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",\n  \"token_uri\": \"https://accounts.google.com/o/oauth2/token\",\n  \"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\",\n  \"client_x509_cert_url\": \"https://www.googleapis.com/robot/v1/metadata/x509/pet-110347448408766049948%40broad-dsde-dev.iam.gserviceaccount.com\"\n}",
        |  "final_workflow_log_dir": "final_workflow_log_dir",
        |  "read_from_cache": true,
        |  "delete_intermediate_output_files": true,
        |  "use_reference_disks": true,
        |  "memory_retry_multiplier": 2.718,
        |  "backend": "PAPIv2",
        |  "ignore_empty_outputs": false,
        |  "monitoring_script": "script",
        |  "monitoring_image": "image",
        |  "monitoring_image_script": "image script"
        |}
      """.stripMargin.parseJson

    val noFailureModeJson = noFailureMode.toJson
    noFailureModeJson should equal(expectedJsonNoFailureMode)
    noFailureModeJson.convertTo[ExecutionServiceWorkflowOptions] should equal(noFailureMode)
  }

}
