package org.broadinstitute.dsde.rawls.model

import java.util.UUID

import bio.terra.workspace.model.CloningInstructionsEnum.NOTHING
import bio.terra.workspace.model.ReferenceTypeEnum.DATA_REPO_SNAPSHOT
import bio.terra.workspace.model.{DataReferenceDescription, DataReferenceList, DataRepoSnapshot, UpdateDataReferenceRequestBody}
import org.broadinstitute.dsde.rawls.model.DataReferenceModelJsonSupport._
import spray.json._

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class DataReferenceModelSpec extends AnyFreeSpec with Matchers {

  "DataReferenceModel" - {
    "stringOrNull() does the right thing" in {
      assertResult(JsNull) {
        stringOrNull(null)
      }
      assertResult(JsString("x")) {
        stringOrNull("x")
      }
      assertResult(JsString(DATA_REPO_SNAPSHOT.toString)) {
        stringOrNull(DATA_REPO_SNAPSHOT)
      }
    }

    "JSON logic" - {

      "DataReferenceDescriptionList, which contains DataReferenceDescription, which contains DataRepoSnapshot" in {
        val referenceId = UUID.randomUUID()
        val workspaceId = UUID.randomUUID()
        assertResult {
          s"""{"resources":[{"referenceId": "$referenceId","name":"test-ref","workspaceId":"$workspaceId","referenceType":"$DATA_REPO_SNAPSHOT","reference":{"instanceName":"test-instance","snapshot":"test-snapshot"},"description":null,"cloningInstructions":"$NOTHING"}]}""".parseJson
        } {
          new DataReferenceList().resources(ArrayBuffer(
            new DataReferenceDescription()
              .referenceId(referenceId)
              .name("test-ref")
              .workspaceId(workspaceId)
              .referenceType(DATA_REPO_SNAPSHOT)
              .reference(new DataRepoSnapshot().instanceName("test-instance").snapshot("test-snapshot"))
              .cloningInstructions(NOTHING)
          ).asJava).toJson
        }
      }

      "DataReferenceDescription with bad UUID's should fail" in {
        assertThrows[DeserializationException] {
          s"""{"referenceId": "abcd","name":"test-ref","workspaceId":"abcd","referenceType":"$DATA_REPO_SNAPSHOT","reference":{"instanceName":"test-instance","snapshot":"test-snapshot"},"cloningInstructions":"$NOTHING"}""".parseJson.convertTo[DataReferenceDescription]
        }
      }

      "UpdateDataReferenceRequestBody should work with only one parameter" in {
        assertResult { s"""{"name":null,"description":"foo"}""".parseJson } {
          new UpdateDataReferenceRequestBody().description("foo").toJson
        }
      }

      "UpdateDataReferenceRequestBody with no parameters should fail" in {
        assertThrows[DeserializationException] {
          s"""{}""".parseJson.convertTo[UpdateDataReferenceRequestBody]
        }
      }
    }
  }
}
