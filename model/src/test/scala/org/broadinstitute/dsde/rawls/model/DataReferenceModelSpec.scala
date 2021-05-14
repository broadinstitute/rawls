package org.broadinstitute.dsde.rawls.model

import java.util.UUID

import bio.terra.workspace.model.CloningInstructionsEnum.NOTHING
import bio.terra.workspace.model.ReferenceTypeEnum.DATA_REPO_SNAPSHOT
import bio.terra.workspace.model.{CloningInstructionsEnum, DataReferenceDescription, DataReferenceList, DataRepoSnapshot, DataRepoSnapshotAttributes, DataRepoSnapshotResource, GcpBigQueryDatasetAttributes, ResourceAttributesUnion, ResourceDescription, ResourceList, ResourceMetadata, ResourceType, StewardshipType, UpdateDataReferenceRequestBody}
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

      "DataRepoSnapshotResource, ResourceMetadata, DataRepoSnapshotAttributes" in {
        val resourceId = UUID.randomUUID()
        val workspaceId = UUID.randomUUID()
        val snapshotId = UUID.randomUUID()
        assertResult {
          s"""{
              "attributes": {
                             "instanceName": "test-instance",
                             "snapshot": "$snapshotId"
              },
              "metadata": {
                            "workspaceId": "$workspaceId",
                            "resourceId": "$resourceId",
                            "name": "testReference",
                            "description": "hello",
                            "resourceType": "DATA_REPO_SNAPSHOT",
                            "stewardshipType": "REFERENCED",
                            "cloningInstructions": "COPY_NOTHING"
              }
             }
          """.parseJson
        } {
          new DataRepoSnapshotResource()
            .metadata(
              new ResourceMetadata()
                .name("testReference")
                .resourceId(resourceId)
                .workspaceId(workspaceId)
                .description("hello")
                .resourceType(ResourceType.DATA_REPO_SNAPSHOT)
                .stewardshipType(StewardshipType.REFERENCED)
                .cloningInstructions(CloningInstructionsEnum.NOTHING)
            )
            .attributes(
              new DataRepoSnapshotAttributes()
                .instanceName("test-instance")
                .snapshot(snapshotId.toString)
            ).toJson
        }

      }

      "ResourceList, ResourceDescription, ResourceMetadata, ResourceAttributesUnion, DataRepoSnapshotAttributes" in {
        val snapshotResourceId = UUID.randomUUID()
        val workspaceId = UUID.randomUUID()
        val snapshotId = UUID.randomUUID()
        assertResult {
          s"""
             {
               "resources": [
                             {
                               "metadata": {
                                             "cloningInstructions":"COPY_NOTHING",
                                             "description":"im a lil snapshot",
                                             "name":"snapshot1",
                                             "resourceId":"$snapshotResourceId",
                                             "resourceType":"DATA_REPO_SNAPSHOT",
                                             "stewardshipType":"REFERENCED",
                                             "workspaceId":"$workspaceId"
                                            },
                               "resourceAttributes": { "gcpDataRepoSnapshot": {
                                                                                "instanceName":"test-instance",
                                                                                "snapshot":"$snapshotId"
                                                                               }
                                                     }
                             }
                            ]
             }
             """.parseJson
        } {
          new ResourceList().resources(
            List(
              new ResourceDescription()
                .metadata(
                  new ResourceMetadata()
                    .name("snapshot1")
                    .description("im a lil snapshot")
                    .resourceId(snapshotResourceId)
                    .resourceType(ResourceType.DATA_REPO_SNAPSHOT)
                    .stewardshipType(StewardshipType.REFERENCED)
                    .workspaceId(workspaceId)
                    .cloningInstructions(CloningInstructionsEnum.NOTHING)

                )
                .resourceAttributes(
                  new ResourceAttributesUnion()
                    .gcpDataRepoSnapshot(
                      new DataRepoSnapshotAttributes()
                        .instanceName("test-instance")
                        .snapshot(snapshotId.toString)
                    ))
            ).asJava
          ).toJson
        }
      }

      "Parsing ResourceMetadata should fail if resource id string is not a UUID" in {
        val workspaceId = UUID.randomUUID()
        assertThrows[DeserializationException] {
          s"""
             {
               "cloningInstructions": "COPY_NOTHING",
               "description": "im a lil snapshot",
               "name": "snapshot1",
               "resourceId": "not-an-id",
               "resourceType": "DATA_REPO_SNAPSHOT",
               "stewardshipType": "REFERENCED",
               "workspaceId": "$workspaceId"
              }""".parseJson.convertTo[ResourceMetadata]
        }
      }

      "should fail if workspace id string is not a UUID" in {
        val resourceId = UUID.randomUUID()
        assertThrows[DeserializationException] {
          s"""
             {
               "cloningInstructions":"COPY_NOTHING",
               "description": "im a lil snapshot",
               "name": "snapshot1",
               "resourceId": "$resourceId",
               "resourceType": "DATA_REPO_SNAPSHOT",
               "stewardshipType": "REFERENCED",
               "workspaceId": "not-an-id"
              }""".parseJson.convertTo[ResourceMetadata]
        }
      }

      "UpdateDataReferenceRequestBody should work when updating name and description" in {
        assertResult { s"""{"name":"foo","description":"bar"}""".parseJson } {
          new UpdateDataReferenceRequestBody().name("foo").description("bar").toJson
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
